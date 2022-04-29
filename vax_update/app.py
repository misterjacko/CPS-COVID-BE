import requests
import logging
import pandas as pd
import boto3
import os
from io import StringIO
from collections import deque
from datetime import datetime



logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3client = boto3.client('s3')
cloudfrontclient = boto3.client('cloudfront')


def import_dataset(url):
    df = pd.read_csv(url)
    return df


def refresh_data(df, data):
    for index, row in df.iterrows():
        school_id = row["CPS_School_ID"]
        try:
            df.at[index, ["Student_Count"]] = data[school_id]["Student_Count"]
            df.at[index, ["Vax_First_Dose"]] = data[school_id]["Vax_First_Dose"]
            # handles case with "None" vaccine data
            if data[school_id]["Vax_Complete"] == None:
                df.at[index, ["Vax_Complete"]] = 0
            else:
                df.at[index, ["Vax_Complete"]] = data[school_id]["Vax_Complete"]
        except Exception as e:
            logger.info("School ID in dataset but not in school API: {}".format(e))
            continue
    return df


def call_api(api):
    vax_dict = {}
    response = requests.get(api)
    for school in response.json():
        vax_dict[school["SchoolID"]] = {
            "Student_Count": school["OverallStudentCount"],
            "Vax_Complete": school["OverallCompletedCount"],
            "Vax_First_Dose": school["OverallOneDoseCount"],
        }
    return vax_dict


def export(df, fileName):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    putLocation = "cpscovid.com"
    putKey = "data/" + fileName
    tagging = "lifecycle=test"
    response = s3client.put_object(Body=csv_buffer.getvalue(), Bucket=putLocation, Key=putKey, Tagging=tagging)
    logger.info(response)


def transposeDf (df):
    begindex = deque(list(df.columns))
    begindex.popleft() 

    transposed = df.transpose()
    new_header = transposed.iloc[0]
    transposed = transposed[1:]
    transposed.columns = new_header
    transposed.insert(0, 'School', begindex, allow_duplicates = True)
    return transposed


def export_to_s3(df, update_time):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    putLocation = "cpscovid.com"
    putKey = "cover/{0}.csv".format(update_time)
    logger.info(update_time)
    response = s3.put_object(Body=csv_buffer.getvalue(), Bucket=putLocation, Key=putKey)
    logger.info(response)


def invalidateCache():
    cloudfrontID = os.environ['cloudfrontCache']
    response = cloudfrontclient.create_invalidation(
        DistributionId = cloudfrontID,
        InvalidationBatch = {
            'Paths': {
                'Quantity': 2,
                'Items': [
                    '/data/allCpsCovidData.csv',
                    '/data/newFormatTest.csv',
                ]
            },
            'CallerReference': str(datetime.now())
        }
    )
    return response


def lambda_handler(event, context):
    vax_api = "https://api.cps.edu/health/cps/SchoolCOVIDStudentVaccinationRate"
    vax_data = call_api(vax_api)

    data_url = "https://s3.amazonaws.com/cpscovid.com/data/allCpsCovidData.csv"
    dataset = import_dataset(data_url)

    dataset = refresh_data(dataset, vax_data)
    transposed = transposeDf(dataset)

    export(dataset, "allCpsCovidData.csv")
    export(transposed, "newFormatTest.csv")
    
    logger.info(invalidateCache())
