import boto3, json, requests, logging
import pandas as pd
from io import StringIO


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def parse_date(date):
    # figure out how to 
    return date

def make_df(response):
    return pd.read_json(json.dumps(response))

def export_to_s3(df, update_time):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    putLocation = "cpscovid.com"
    putKey = "cover/{0}.csv".format(update_time)
    logger.info(update_time)
    response = s3.put_object(Body=csv_buffer.getvalue(), Bucket=putLocation, Key=putKey)
    logger.info(response)

def lambda_handler(event, context):
    logger.info("begin")
    response = requests.get("https://api.cps.edu/health/cps/School2021DailyCovidActionable")
    response = response.json()
    df = make_df(response)
    update_time = response[0]["LastRefreshed"].replace(":", ".")
    export_to_s3(df, update_time)
