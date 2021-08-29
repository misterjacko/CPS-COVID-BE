import boto3
import logging
import pandas as pd
import os
from io import StringIO
from collections import Counter
from datetime import datetime, timedelta
from collections import deque

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3client = boto3.client('s3')
snsclient = boto3.client('sns')

def newDataQuialityControl(freshurl):
    fresh = pd.read_csv(freshurl)
    # checks dimensions raw shape should be (517, 6)
    assert fresh.shape == (517, 6), "New dataframe is the wrong shape!"
    # makes sure schools are in same order
    fresh[fresh.columns[0]] = fresh[fresh.columns[0]].apply(lambda x: formatSchoolNames(x))
    totalsdf = pd.read_csv("https://s3.amazonaws.com/cpscovid.com/data/allCpsCovidData.csv")
    assert fresh[fresh.columns[0]].equals(totalsdf[totalsdf.columns[0]]), "Table order rearranged!"

def downloadNewData (): # returns data as panndas.df
    # old sheet
    # sheetID = '1dMtr8hhhKjPyyNg7i6V52iMQXEqa67E9iAmECeOqZ6c'
    # worksheetName = 'COVID' # only takes first word fo the sheet
    # case data
    sheetID = '1urvD6F5RD3U3VHImFc5Kfv63lV7UQ9ivv_HpiFobGVA'
    worksheetName = 'case' # only takes first word fo the sheet
    freshurl = 'https://docs.google.com/spreadsheets/d/{0}/gviz/tq?tqx=out:csv&sheet={1}'.format(
        sheetID,
        worksheetName
    )

    # validate new data
    # will need to some it with a new way of validating
    #newDataQuialityControl(freshurl)



    fresh = pd.read_csv(freshurl)

    # remove entries with no school ID
    fresh = fresh.dropna(subset=['CPS School ID'])

    return (fresh)

def checkLastColumn(olddf, formated, updateChecker):
    # format todays date. )
    if formated not in olddf.columns:
        olddf[formated] = 0
        updateChecker = True
        return olddf, updateChecker
    else:
        return olddf, updateChecker

def updateOldDf(olddf, fresh, formated, updateChecker):
    # Total all cases from each school
    # how do we skip this if there are not new numbers?
    # for now just keep it
    freshTotals = Counter(fresh['CPS School ID'])
    # determines the column indexes for tail sums 
    end = len(olddf.columns)
    d7 = end - 7
    d14 = end - 13
    # Primary df update
    for index, row in olddf.iterrows():
        # determines if there is and difference between old totals and fresh totals
        if row['gTotal'] != freshTotals[row['CPS_School_ID']]:
            #updates daily number and total
            updateChecker = True
            olddf.at[index,formated] = freshTotals[row['CPS_School_ID']] - row['gTotal'] + row[formated]
            olddf.at[index,['gTotal']] = freshTotals[row['CPS_School_ID']]

        # updates 7 and 14 day totals regardless of new
        # 
        olddf.at[index, ['7Total']] = olddf.iloc[index, d7:end].sum()
        olddf.at[index, ['14Total']] = olddf.iloc[index, d14:end].sum()

    return olddf, updateChecker
def updateOldTotals(oldtotals, newdaily, formated):

    if str(oldtotals.at[len(oldtotals)-1,'date']) == formated:
        oldtotals.drop(oldtotals.tail(1).index,inplace=True)

    newrunning = oldtotals['daily'].sum() + newdaily
    new7DaySum = (oldtotals['daily'].tail(6).sum() + newdaily)
    new7DayAvg = new7DaySum / 7
    new14DaySum = (oldtotals['daily'].tail(13).sum() + newdaily)
    new14DayAvg = new14DaySum / 14
    newrow = [formated, newdaily, newrunning, new7DaySum, new7DayAvg, new14DaySum, new14DayAvg]
    oldtotals.loc[len(oldtotals)] = newrow

    return oldtotals

def transposeDf (df):
    begindex = deque(list(df.columns))
    begindex.popleft() 

    transposed = df.transpose()
    new_header = transposed.iloc[0]
    transposed = transposed[1:]
    transposed.columns = new_header
    transposed.insert(0, 'School', begindex, allow_duplicates = True)
    return transposed
def updateOldData(fresh):
    updateChecker = False
    time = datetime.now() - timedelta(hours=5)
    formated = time.strftime("%Y%m%d")

    olddf = pd.read_csv("https://s3.amazonaws.com/cpscovid.com/data/allCpsCovidData.csv")
    oldtotals = pd.read_csv("https://s3.amazonaws.com/cpscovid.com/data/CPStotals.csv")
    
    # ensures today's date is in olddf
    olddf, updateChecker = checkLastColumn(olddf, formated, updateChecker)

    # this function will update the master list of all cases 
    olddf, updateChecker = updateOldDf (olddf, fresh, formated, updateChecker)

    if updateChecker:
        # update cpstotals
        newdaily = olddf[formated].sum()
        oldtotals = updateOldTotals(oldtotals, newdaily, formated)


        # make transposed df for easier parsing for front end maybe make a function
        transposed = transposeDf(olddf)


        # export to csv
        exportUpdated(olddf, 'allCpsCovidData.csv')
        exportUpdated(oldtotals, 'CPStotals.csv')
        exportUpdated(transposed, 'newFormatTest.csv')
        logger.info(sendSNS()) # this can be passed an array for subscription tags 
    else:
        logger.info("no update")
    

def sendSNS():
    updateARN = os.environ['snsTopicArn']
    response = snsclient.publish(
        TopicArn=updateARN,
        Message='test update. please ignore'
    )
    return response

def exportUpdated(updated, fileName):
    csv_buffer = StringIO()
    updated.to_csv(csv_buffer, index=False)
    putLocation = "cpscovid.com"
    putKey = "data/" + fileName
    tagging = "lifecycle=true"
    logger.info(fileName)
    response = s3client.put_object(Body=csv_buffer.getvalue(), Bucket=putLocation, Key=putKey, Tagging=tagging)
    logger.info(response)

def findCSVDate(date):
    return (datetime.strftime(datetime.strptime(date, "%m/%d/%Y") + timedelta(days=6), "%Y%m%d"))

def lambda_handler(event, context):
    fresh = downloadNewData()
    updateOldData(fresh)
    return ('COMPLETE')
