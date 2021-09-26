import boto3
import requests
from bs4 import BeautifulSoup as bs4
import logging
import pandas as pd
import os
import tweepy
from io import StringIO
from collections import Counter
from datetime import datetime, timedelta
from collections import deque


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3client = boto3.client('s3')
snsclient = boto3.client('sns')
cloudfrontclient = boto3.client('cloudfront')

def newDataQuialityControl(freshurl):
    fresh = pd.read_csv(freshurl)
    # checks dimensions raw shape should be (517, 6)
    assert fresh.shape == (517, 6), "New dataframe is the wrong shape!"
    # makes sure schools are in same order
    fresh[fresh.columns[0]] = fresh[fresh.columns[0]].apply(lambda x: formatSchoolNames(x))
    totalsdf = pd.read_csv("https://s3.amazonaws.com/cpscovid.com/data/allCpsCovidData.csv")
    assert fresh[fresh.columns[0]].equals(totalsdf[totalsdf.columns[0]]), "Table order rearranged!"

def downloadNewData (): # returns data as panndas.df
    start_date = "8-29-2021"
    response = requests.get("	https://api.cps.edu/health/cps/schoolWeeklyCovidActionable?startdate={0}".format(start_date))
    fresh = response.json()
    return (fresh)

def checkLastColumn(olddf, formated, updateChecker):
    # format todays date. )
    if formated not in olddf.columns:
        olddf[formated] = 0
        updateChecker = True
        return olddf, updateChecker
    else:
        return olddf, updateChecker

def updateOldDf(olddf, fresh, formated, updateChecker, updateNumbers):
    # Total all cases from each school
    # how do we skip this if there are not new numbers?
    # for now just keep it
    newCaseDict = {}
    # freshTotals = Counter(fresh['CPS School ID'])

    #makes a dict of school totals with schoolid = casetotals
    freshTotals = {}
    for week in fresh:
        if week['SchoolID'] not in freshTotals:
            freshTotals[week['SchoolID']] = week['TotalCaseCount']
        else:
            freshTotals[week['SchoolID']] += week['TotalCaseCount']
    # print(freshTotals)
    # determines the column indexes for tail sums 
    end = len(olddf.columns)
    d7 = end - 7
    d14 = end - 13
    d21 = end - 20
    # Primary df update

    for index, row in olddf.iterrows():
        if row['CPS_School_ID'] not in freshTotals:
            continue
        # determines if there is and difference between old totals and fresh totals
        if row['gTotal'] - row['preSY2122'] != freshTotals[row['CPS_School_ID']]:
            yearTotal = row['gTotal'] - row['preSY2122']
            # print("not")
            #updates daily number and total
            updateChecker = True
            updateNumbers = True

            olddf.at[index,formated] = freshTotals[row['CPS_School_ID']] - yearTotal + row[formated]
            olddf.at[index,['gTotal']] = freshTotals[row['CPS_School_ID']] + row['preSY2122']

        olddf.at[index, ['7Total']] = olddf.iloc[index, d7:end].sum()
        olddf.at[index, ['14Total']] = olddf.iloc[index, d14:end].sum()
        olddf.at[index, ['21Total']] = olddf.iloc[index, d21:end].sum()
    if updateNumbers:
        for index, row in olddf.iterrows():
            if row[formated] != 0:
                schoolName = row['School']
                newCases = row[formated]
                schoolLat = row['Latitude']
                schoolLong = row['Longitude']

                properties = [newCases, schoolLat, schoolLong]
                newCaseDict[schoolName] = properties

    return olddf, updateChecker, updateNumbers, newCaseDict
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

def formatSNS(data, table_date):
    dataString = "<caption>Date: {0}</caption>".format(table_date)
    dataString = "{0}<tr><th>School</th><th>New Cases</th></tr>".format(dataString)
    schoolCount = len(data)
    caseCount = 0
    for item in data:
        caseCount += data[item][0]
        schoolURL = '?name={0}&Lat={1}&Long={2}'.format(item.replace(' ', '_'), data[item][1], data[item][2])
        linkHTML = '<a href=./school.html{0}>{1}</a>'.format(schoolURL, item)
        line = '<tr><td>{0}</td><td>{1}</td></tr>'.format(linkHTML, data[item][0])
        dataString = '{0}{1}'.format(dataString, line)
    sns_string = 'cpscovid.com has detected new COVID-19 cases.\n\n'
    sns_string = '{0}{1} new cases reported.\n'.format(sns_string, caseCount)
    sns_string = '{0}{1} schools affected.\n\n'.format(sns_string, schoolCount)
    sns_string = '{0}View a list of newly published cases at cpscovid.com/newcases.html\n\n'.format(sns_string)
    sns_string = '{0}Follow @CPSCovid on twitter at twitter.com/CPSCovid'.format(sns_string)

    tweet_string = 'Case numbers updated by @ChiPubSchools.\n\n'
    tweet_string = '{0}Cumulative cases reported {1}\n'.format(tweet_string, table_date)
    tweet_string = '{0}New cases: {1}\n'.format(tweet_string, caseCount)
    tweet_string = '{0}Schools affected: {1}\n\n'.format(tweet_string, schoolCount)
    tweet_string = '{0}View the list of cases at cpscovid.com/newcases.html'.format(tweet_string)
    return dataString, sns_string, tweet_string

def updateOldData(fresh):
    updateChecker = False
    updateNumbers = False
    time = datetime.now() - timedelta(hours=5)
    formated = time.strftime("%Y%m%d")
    table_date = time.strftime("%b %d %Y")

    olddf = pd.read_csv("https://s3.amazonaws.com/cpscovid.com/data/allCpsCovidData.csv")
    oldtotals = pd.read_csv("https://s3.amazonaws.com/cpscovid.com/data/CPStotals.csv")
    
    # ensures today's date is in olddf
    olddf, updateChecker = checkLastColumn(olddf, formated, updateChecker)

    # this function will update the master list of all cases
    olddf, updateChecker, updateNumbers, newCaseDict = updateOldDf (olddf, fresh, formated, updateChecker, updateNumbers)

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
        if updateNumbers:          
            dataString, sns_string, tweet_string = formatSNS(newCaseDict, table_date)
            exportHtml(dataString)
            logger.info(sendSNS(sns_string)) # this requires topicARN and wont work in test
            invalidateCache()
            sendTweet(tweet_string)
            logger.info(tweet_string)
            logger.info(sns_string)

            freshdf = pd.DataFrame.from_dict(fresh, orient='columns')
            exportUpdated(freshdf, 'dataFromCPS.csv')
        
    else:
        logger.info("no update")

def sendTweet(tweet_string):
    consumer_key = os.environ['consumer_key']
    consumer_secret = os.environ['consumer_secret']
    access_token = os.environ['access_token']
    access_token_secret = os.environ['access_token_secret']

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)  
    api.update_status(status = tweet_string)


def exportHtml(dataString):
    page = requests.get('https://s3.amazonaws.com/cpscovid.com/newcasesTemplate.html')
    page = page.text
    page = page.replace('ZrIxb9bnehGHnFuptWw8', dataString)

    soup = bs4(page, 'html.parser')

    putLocation = 'cpscovid.com'
    # putKey = "newcasestest.html"
    putKey = "newcases.html"
    tagging = "lifecycle=true"
    acl = 'public-read'
    ctype = "text/html"
    logger.info(putKey)
    response = s3client.put_object(Body=str(soup.prettify()), Bucket=putLocation, Key=putKey, Tagging=tagging, ACL=acl, ContentType=ctype)
    logger.info(response)
    
def invalidateCache():
    cloudfrontID = os.environ['cloudfrontCache']
    response = cloudfrontclient.create_invalidation(
        DistributionId = cloudfrontID,
        InvalidationBatch = {
            'Paths': {
                'Quantity': 2,
                'Items': [
                    '/data/*',
                    '/newcases.html'
                ]
            },
            'CallerReference': str(datetime.now())
        }
    )
    return response

def sendSNS(snsMessage):
    updateARN = os.environ['snsTopicArn']
    response = snsclient.publish(
        TopicArn = updateARN,
        Subject = 'COVID update',
        Message = snsMessage
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
