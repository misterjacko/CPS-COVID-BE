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


def refresh_data(df, school_data, vax_data):
    for index, row in df.iterrows():
        school_id = row["CPS_School_ID"]
        
        try:

            if row["School"] != school_data[school_id]["short_name"]:
                print ("{}  -->  {}".format(row["School"], school_data[school_id]["short_name"]))
            df.at[index, ["School"]] = school_data[school_id]["short_name"]
            df.at[index, ["Latitude"]] = school_data[school_id]["latitude"]
            df.at[index, ["Longitude"]] = school_data[school_id]["longitude"]
            df.at[index, ["Student_Count"]] = vax_data[school_id]["student_count"]
            df.at[index, ["Vax_First_Dose"]] = vax_data[school_id]["vax_first_dose"]
            # handles case with "None" vaccine data
            if vax_data[school_id]["vax_complete"] == None:
                df.at[index, ["Vax_Complete"]] = 0
            else:
                df.at[index, ["Vax_Complete"]] = vax_data[school_id]["vax_complete"]
        except Exception as e:
            logger.info("School ID in dataset but not in school API: {}".format(e))
            continue
    return df


def call_vax(api):
    vax_dict = {}
    response = requests.get(api)
    for school in response.json():
        vax_dict[school["SchoolID"]] = {
            "student_count": school["OverallStudentCount"],
            "vax_complete": school["OverallCompletedCount"],
            "vax_first_dose": school["OverallOneDoseCount"],
        }
    return vax_dict


def call_school(api):
    school_dict = {}
    response = requests.get(api)
    for school in response.json():
        school_dict[school["SchoolID"]] = {
            "short_name": school["SchoolShortName"].replace(" - ", "-"), # .replace(" ES", " ELEMENTARY SCHOOL").replace(" HS", " HIGH SCHOOL"),
            "latitude": school["AddressLatitude"],
            "longitude": school["AddressLongitude"],
        }
    return school_dict

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
    vax_data = call_vax(vax_api)

    school_api = "https://api.cps.edu/schoolprofile/cps/AllSchoolProfiles"
    school_data = call_school(school_api)

    data_url = "https://s3.amazonaws.com/cpscovid.com/data/allCpsCovidData.csv"
    dataset = import_dataset(data_url)

    dataset = refresh_data(dataset, school_data, vax_data)
    # dataset.sort_values(by=['School'], inplace=True, ignore_index=True)
    # transposed = transposeDf(dataset)

    # export(dataset, "allCpsCovidData.csv")
    # export(transposed, "newFormatTest.csv")
    
    # logger.info(invalidateCache())
