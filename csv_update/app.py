import boto3
import logging
import pandas as pd
from io import StringIO
from collections import Counter
from datetime import datetime, timedelta
from collections import deque

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3client = boto3.client('s3')

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

    # prob wont be able to add to a df now. Will need to do a for loop through the list, 


    fresh = pd.read_csv(freshurl)

    # remove entries with no school ID
    fresh = fresh.dropna(subset=['CPS School ID'])

    return (fresh)

def updateOldData(fresh):
    olddf = pd.read_csv("https://s3.amazonaws.com/cpscovid.com/data/allCpsCovidData.csv")
    oldtotals = pd.read_csv("https://s3.amazonaws.com/cpscovid.com/data/CPStotals.csv")
    # currentColumn = list(fresh.columns)[len(fresh.columns)-1]
    
    # make list of acceptable school IDs
    schoolIDList = olddf.CPS_School_ID.tolist()

    # Total all cases from each school
    freshTotals = Counter(fresh['CPS School ID'])

    # lambda uses UTC so numbers checked "today" will be for "yesterday" 
    # Determines date makes new column and populates with 0
    yesterday = datetime.today() - timedelta(days=1)
    formated = yesterday.strftime("%Y%m%d")
    olddf[formated] = 0

    # determines the column indexes for tail sums 
    end = len(olddf.columns)
    d7 = end - 7
    d14 = end - 13

    # Primary df update
    for index, row in olddf.iterrows():
        # determines if there is and difference between old totals and fresh totals
        if row['gTotal'] != freshTotals[row['CPS_School_ID']]:
            #updates daily number and total
            olddf.at[index,formated]= freshTotals[row['CPS_School_ID']] - row['gTotal']
            olddf.at[index,['gTotal']] = freshTotals[row['CPS_School_ID']]
        # updates 7 and 14 day totals regardless of new
        olddf.at[index, ['7Total']] = olddf.iloc[index, d7:end].sum()
        olddf.at[index, ['14Total']] = olddf.iloc[index, d14:end].sum()

    # update cpstotals
    newdaily = olddf[formated].sum()
    newrunning = oldtotals['daily'].sum() + newdaily
    new7DaySum = (oldtotals['daily'].tail(6).sum() + newdaily)
    new7DayAvg = new7DaySum / 7
    new14DaySum = (oldtotals['daily'].tail(13).sum() + newdaily)
    new14DayAvg = new14DaySum / 14

    newrow = [formated, newdaily, newrunning, new7DaySum, new7DayAvg, new14DaySum, new14DayAvg]
    oldtotals.loc[len(oldtotals)] = newrow

    # make transposed df for easier parsing for front end maybe make a function
    begindex = deque(list(olddf.columns))
    begindex.popleft() 


    transposed = olddf.transpose()
    new_header = transposed.iloc[0]
    transposed = transposed[1:]
    transposed.columns = new_header
    transposed.insert(0, 'School', begindex, allow_duplicates = True)

    # export to csv
    exportUpdated(olddf, 'allCpsCovidData.csv')
    exportUpdated(oldtotals, 'CPStotals.csv')
    exportUpdated(transposed, 'newFormatTest.csv')

def exportUpdated(updated, fileName):
    csv_buffer = StringIO()
    updated.to_csv(csv_buffer, index=False)
    putLocation = "cpscovid.com"
    putKey = "data/" + fileName
    logger.info(fileName)
    response = s3client.put_object(Body=csv_buffer.getvalue(), Bucket=putLocation, Key=putKey)
    logger.info(response)

def findCSVDate(date):
    return (datetime.strftime(datetime.strptime(date, "%m/%d/%Y") + timedelta(days=6), "%Y%m%d"))

def lambda_handler(event, context):
    fresh = downloadNewData()
    updateOldData(fresh)
    return ('COMPLETE')