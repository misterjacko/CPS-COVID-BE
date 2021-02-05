import boto3
import logging
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta

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
    sheetID = '1dMtr8hhhKjPyyNg7i6V52iMQXEqa67E9iAmECeOqZ6c'
    worksheetName = 'COVID' # only takes first word fo the sheet
    freshurl = 'https://docs.google.com/spreadsheets/d/{0}/gviz/tq?tqx=out:csv&sheet={1}'.format(
        sheetID,
        worksheetName
    )

    # validate new data
    newDataQuialityControl(freshurl)

    fresh = pd.read_csv(freshurl,skiprows=[0] , names=["School", "Q3 SY20", "Q4 SY20", "Summer SY20", "Q1 SY21", "Q2 SY21"])
    fresh.School = fresh.School.apply(lambda x: formatSchoolNames(x))
    return (fresh)

def updateOldData(fresh):
    olddf = pd.read_csv("https://s3.amazonaws.com/cpscovid.com/data/allCpsCovidData.csv")
    oldtotals = pd.read_csv("https://s3.amazonaws.com/cpscovid.com/data/CPStotals.csv")
    currentColumn = list(fresh.columns)[len(fresh.columns)-1]
    
    # lambda uses UTC so numbers checked "today" will be for "yesterday" 
    yesterday = datetime.today() - timedelta(days=1)
    formated = yesterday.strftime("%Y%m%d")

    olddf[formated] = fresh[currentColumn] - olddf[currentColumn]
    olddf[currentColumn] = fresh[currentColumn]

    # update cpstotals
    newdaily = olddf[formated].sum()
    newrunning = oldtotals['daily'].sum() + newdaily
    new7day = (oldtotals['daily'].tail(6).sum() + newdaily) / 7

    newrow = [formated, newdaily, newrunning, int(round(new7day))]
    oldtotals.loc[len(oldtotals)] = newrow

    # make transposed df for easire parsing for front end
    transposed = olddf.transpose()
    new_header = transposed.iloc[0]
    transposed = transposed[1:]
    transposed.columns = new_header

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

def formatSchoolNames(i):
    i = i.replace(" HS", " HIGH SCHOOL")
    i = i.replace(" ES", " ELEMENTARY SCHOOL")
    return i.upper()

def lambda_handler(event, context):
    fresh = downloadNewData()
    updateOldData(fresh)
    return ('COMPLETE')
