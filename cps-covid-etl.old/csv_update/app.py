import boto3
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta

def downloadNewData (): #returns data as panndas.df
    sheetID = '1dMtr8hhhKjPyyNg7i6V52iMQXEqa67E9iAmECeOqZ6c'
    worksheetName = 'COVID'
    testurl = 'https://docs.google.com/spreadsheets/d/{0}/gviz/tq?tqx=out:csv&sheet={1}'.format(
        sheetID,
        worksheetName
    )

    fresh = pd.read_csv(testurl,skiprows=[0], names=["School", "Q3 SY20", "Q4 SY20", "Summer SY20", "Q1 SY21", "Q2 SY21"])
    fresh.School = fresh.School.apply(lambda x: formatSchoolNames(x))
    return (fresh)

def updateOldData(fresh):
    olddf = pd.read_csv("https://s3.amazonaws.com/cpscovid.com/data/allCpsCovidData.csv")
    currentColumn = list(fresh.columns)[len(fresh.columns)-1]
    
    #lambda uses UTC so numbers checked "today" will be for "yesterday" 
    yesterday = datetime.today() - timedelta(days=1)
    formated = yesterday.strftime("%Y%m%d")

    olddf[formated] = fresh[currentColumn] - olddf[currentColumn]
    olddf[currentColumn] = fresh[currentColumn]
    return (olddf)

def exportUpdated(updated):
    csv_buffer = StringIO()
    updated.to_csv(csv_buffer, index=False)
    client = boto3.client('s3')
    putLocation = "cpscovid.com"
    putKey = "data/allCpsCovidData.csv"
    response = client.put_object(Body=csv_buffer.getvalue(), Bucket=putLocation, Key=putKey)
    return response

def formatSchoolNames(i):
    i = i.replace(" HS", " HIGH SCHOOL")
    i = i.replace(" ES", " ELEMENTARY SCHOOL")
    return i.upper()

def lambda_handler(event, context):
    fresh = downloadNewData()
    updated = updateOldData(fresh)
    return exportUpdated(updated)
