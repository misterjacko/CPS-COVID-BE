import boto3
import json
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
dynamodb = boto3.client('dynamodb')

def newDataQuialityControl(freshurl):
    fresh = pd.read_csv(freshurl)
    # checks dimensions raw shape should be (517, 6)
    assert fresh.shape == (517, 6), "New dataframe is the wrong shape!"
    # makes sure schools are in same order
    fresh[fresh.columns[0]] = fresh[fresh.columns[0]].apply(lambda x: formatSchoolNames(x))
    totalsdf = pd.read_csv("https://s3.amazonaws.com/cpscovid.com/data/allCpsCovidData.csv")
    assert fresh[fresh.columns[0]].equals(totalsdf[totalsdf.columns[0]]), "Table order rearranged!"

def downloadNewData (): # returns data as pandas.df
    start_date = "8-29-2021"
    response = requests.get("https://api.cps.edu/health/cps/School2021DailyCovidActionable?startdate={0}".format(start_date))
    fresh = response.json()
    return (fresh)

def verify_yesterday_cases(olddf, column_date):
    # quick return if there are not a total of 0 cases
    if olddf[column_date].sum() != 0:
        return True
    else:
        # checks in case of a cumulative 0 but still individual case reports
        for row in olddf.iterrows():
            if row[column_date] != 0:
                return True
    logger.info("No cases to Archive in column {}.".format(column_date))
    return False


def export_json():
    yesterday = datetime.now() - timedelta(days=1)
    file_date = yesterday.strftime("%Y-%m-%d")

    bucket = "cpscovid.com"
    old_key = 'data/dataFromCPS.csv'

    logger.info("Downloading yesterday's last api response")
    try:
        old_data = pd.read_csv("https://{}/{}".format(bucket, old_key))
        old_data = json.loads(old_data.to_json(orient="records"))
    except Exception as e:
        logger.info("ERROR: Unable to download yesterday's api response")
        logger.info(e)

    new_key = "data/daily-json/{}.json".format(file_date)
    logger.info("saving yesterday's last api response as json")
    try:
        response = s3client.put_object(
            Bucket=bucket,
            Key=new_key,
            Body=json.dumps(old_data),
        )
        logger.info(response)
    except Exception as e:
        logger.info("ERROR: Unable to save yesterday's api response as json")
        logger.info(e)


def archiveYesterdaysPage(olddf):
    yesterday = datetime.now() - timedelta(days=1)
    file_date = yesterday.strftime("%Y-%m-%d")
    column_date = yesterday.strftime("%Y%m%d")
    case_total = olddf[column_date].sum()

    # if not verify_yesterday_cases(olddf, column_date):
    #     logger.info('No cases to Archive. Skipping Archive.')
    #     return 

    logger.info("Archiving yesterday's case page")
    try:
        move = s3client.copy_object(
            CopySource='cpscovid.com/newcases.html',
            Bucket='cpscovid.com',
            Key='historical/{0}-cases.html'.format(file_date),
            ContentType='text/html',
            ACL='public-read'
        )
        logger.info(move)
    except:
        logger.info('historical export failed')

    # append db

    
    if not verify_yesterday_cases(olddf, column_date):
        logger.info('No cases to Archive. Skipping Archive.')
        return 


    logger.info('Adding entry to table')
    try:
        put_db = dynamodb.put_item(
            TableName = 'cpscovid',
            Item = {
                'item_usage' : {'S':'historical'},
                'page_date': {'S': file_date},
                'total_cases': {'N': str(case_total)},
                'page_url': {'S': 'https://cpscovid.com/historical/{0}-cases.html'.format(file_date)},
            },
        )
        logger.info(put_db)
    except:
        logger.info('DynamoDB append failed')

def checkLastColumn(olddf, formated, updateChecker):
    # format todays date. )
    if formated not in olddf.columns:
        archiveYesterdaysPage(olddf)
        export_json()
        olddf[formated] = 0
        updateChecker = True
        return olddf, updateChecker
    else:
        return olddf, updateChecker

def listOfOldSchools(olddf):
    old_school_list = []
    old_school_list = olddf['CPS_School_ID'].tolist()
    return old_school_list

def GetSchoolInfo(school_id):
    result = requests.get("https://api.cps.edu/schoolprofile/cps/SchoolProfileInformation?SchoolId={0}&SchoolYear=2021".format(school_id))
    school_info = result.json()
    for line in school_info:
        return line["Short_Name"], line["School_Latitude"], line["School_Longitude"]

def updateOldDf(olddf, fresh, formated, updateChecker, updateNumbers):
    # Total all cases from each school
    # how do we skip this if there are not new numbers?
    # for now just keep it
    day_total_dict = {}
    new_update_dict = {}
    # freshTotals = Counter(fresh['CPS School ID'])

    freshTotals = {}
    for week in fresh:
        if week['SchoolID'] not in freshTotals:
            freshTotals[week['SchoolID']] = week['TotalCaseCount']
        else:
            freshTotals[week['SchoolID']] += week['TotalCaseCount']

    logger.info("SY2122 cases processed")
    # determines the column indexes for tail sums 
    end = len(olddf.columns)
    d7 = end - 6
    d14 = end - 13
    d21 = end - 20

    ignore_list = ["610377", "610012", "609703"]
    old_school_list = listOfOldSchools(olddf)
    new_schools = []

    for school_id in freshTotals:
        if (school_id not in old_school_list) & (school_id not in ignore_list):
            new_schools.append(school_id)
    new_school_data =[]
    for school_id in new_schools:
        school_info = GetSchoolInfo(school_id)
        if school_info != None:
            school_name = school_info[0].replace(" - ", "-")
            new_school_data.append([school_name, school_info[1], school_info[2], school_id])
        else:
            logger.info("skipped School ID: {0}".format(school_id)) 

    for new_school in new_school_data:
        while len(new_school) < len(olddf.columns):
            new_school.append(0)
        olddf.loc[len(olddf)] = new_school
    olddf.sort_values(by=['School'], inplace=True, ignore_index=True)


    for index, row in olddf.iterrows():
        if row['CPS_School_ID'] in freshTotals:
            # determines if there is and difference between old totals and fresh totals
            if row['gTotal'] - row['preSY2122'] != freshTotals[row['CPS_School_ID']]:
                year_total = row['gTotal'] - row['preSY2122']
                new_case_number = freshTotals[row['CPS_School_ID']] - year_total
                # print("not")
                #updates daily number and total
                updateChecker = True
                updateNumbers = True

                new_update_dict[row['CPS_School_ID']] = new_case_number
                olddf.at[index,formated] = new_case_number + row[formated]
                olddf.at[index,['gTotal']] = freshTotals[row['CPS_School_ID']] + row['preSY2122']

        # updates windows regardless
        olddf.at[index, ['7Total']] = olddf.iloc[index, d7:end].sum()
        olddf.at[index, ['14Total']] = olddf.iloc[index, d14:end].sum()
        olddf.at[index, ['21Total']] = olddf.iloc[index, d21:end].sum()
    logger.info("New and old cases compared")
    if updateNumbers:
        for index, row in olddf.iterrows():
            if row[formated] != 0:
                schoolName = row['School']
                newCases = row[formated]
                schoolLat = row['Latitude']
                schoolLong = row['Longitude']

                properties = [newCases, schoolLat, schoolLong]
                day_total_dict[schoolName] = properties

    return olddf, updateChecker, updateNumbers, day_total_dict, new_update_dict

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

def formatMessages(day_data, new_data, time):

    date_string = time.strftime("%b. %d, %Y")
    date_time_string = time.strftime("%I:%M %p, %b. %d, %Y")

    new_school_count = len(new_data)
    new_case_count = 0
    for item in new_data:
        new_case_count += new_data[item]

    day_school_count = len(day_data)
    day_case_count = 0  
    dataString = "<tr><th>School</th><th>New Cases</th></tr>"  
    for item in day_data:
        day_case_count += day_data[item][0]
        schoolURL = '?name={0}&Lat={1}&Long={2}'.format(item.replace(' ', '_'), day_data[item][1], day_data[item][2])
        linkHTML = '<a href="http://cpscovid.com/school.html{0}">{1}</a>'.format(schoolURL, item)
        line = '<tr><td>{0}</td><td>{1}</td></tr>'.format(linkHTML, day_data[item][0])
        dataString = '{0}{1}'.format(dataString, line)

    # SNS Email formatting
    sns_string = 'cpscovid.com has detected new COVID-19 cases.\n\n'
    sns_string = '{0}Update: {1}\n'.format(sns_string, date_time_string)
    sns_string = '{0}New cases: {1}\n'.format(sns_string, new_case_count)
    sns_string = '{0}Schools affected: {1}\n\n'.format(sns_string, new_school_count)
    sns_string = '{0}Cumulative cases reported {1}\n'.format(sns_string, date_string)
    sns_string = '{0}New cases: {1}\n'.format(sns_string, day_case_count)
    sns_string = '{0}Schools affected: {1}\n\n'.format(sns_string, day_school_count)
    sns_string = '{0}View the list of cases at cpscovid.com/newcases.html\n\n'.format(sns_string)
    sns_string = '{0}Follow @CPSCovid on twitter at twitter.com/CPSCovid'.format(sns_string)

    # Tweet Formatting
    tweet_string = 'Case numbers updated by @ChiPubSchools.\n\n'
    tweet_string = '{0}Update: {1}\n'.format(tweet_string, date_time_string)
    tweet_string = '{0}New cases: {1}\n'.format(tweet_string, new_case_count)
    tweet_string = '{0}Schools affected: {1}\n\n'.format(tweet_string, new_school_count)
    tweet_string = '{0}Cumulative cases reported {1}\n'.format(tweet_string, date_string)
    tweet_string = '{0}New cases: {1}\n'.format(tweet_string, day_case_count)
    tweet_string = '{0}Schools affected: {1}\n\n'.format(tweet_string, day_school_count)
    tweet_string = '{0}View the list of cases at cpscovid.com/newcases.html\n\n'.format(tweet_string)
    tweet_string = '{0}*this is an auto-generated tweet'.format(tweet_string)
    return dataString, sns_string, tweet_string

def updateOldData(fresh):
    updateChecker = False
    updateNumbers = False
    time = datetime.now() - timedelta(hours=5)
    formated = time.strftime("%Y%m%d")

    olddf = pd.read_csv("https://s3.amazonaws.com/cpscovid.com/data/allCpsCovidData.csv")
    logger.info("S3 Date Downloaded")
    oldtotals = pd.read_csv("https://s3.amazonaws.com/cpscovid.com/data/CPStotals.csv")
    logger.info("S3 Totals Downloaded")
    
    # ensures today's date is in olddf
    olddf, updateChecker = checkLastColumn(olddf, formated, updateChecker)
    logger.info("Last Column Checked for date")
    

    # this function will update the master list of all cases
    olddf, updateChecker, updateNumbers, day_total_dict, new_update_dict = updateOldDf (olddf, fresh, formated, updateChecker, updateNumbers)
    logger.info("Data update Complete")

    if updateChecker:
        # update cpstotals
        newdaily = olddf[formated].sum()
        oldtotals = updateOldTotals(oldtotals, newdaily, formated)

        # # make transposed df for easier parsing for front end maybe make a function
        transposed = transposeDf(olddf)

        # export to csv
        exportUpdated(olddf, 'allCpsCovidData.csv')
        exportUpdated(oldtotals, 'CPStotals.csv')
        exportUpdated(transposed, 'newFormatTest.csv')
        if updateNumbers:          
            dataString, sns_string, tweet_string = formatMessages(day_total_dict, new_update_dict, time)
            exportHtml(dataString, time)
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


def exportHtml(dataString, time):
    date_string = time.strftime("%b. %d, %Y")
    page = requests.get('https://s3.amazonaws.com/cpscovid.com/newcasesTemplate2.html')
    page = page.text
    heading_string = 'Case data collected {0}'.format(date_string)
    page = page.replace('Idd5YYYfjLovtnr7tQjN', date_string)
    page = page.replace('ZQyo3KqWBwvUCb14J884', heading_string)
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
    logger.info("START")
    fresh = downloadNewData()
    logger.info("CPS Data Downloaded")
    updateOldData(fresh)
    return ('COMPLETE')
