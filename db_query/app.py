import boto3
import json
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ddb = boto3.client('dynamodb')

def parse_date(date):
    # figure out how to 
    return date

def query_db(query_value):
    response = ddb.query(
        TableName = 'cpscovid',
        KeyConditionExpression = 'item_usage = :u and begins_with(page_date, :d)',
        ExpressionAttributeValues = {
            ':u': {'S': 'historical'},
            ':d': {'S': query_value},
        }
    )
    return response

def format_response(response):
    resp_dict = {}
    for item in response['Items']:
        item_dict = {'cases': item['total_cases']['N'], 'url':item['page_url']['S']}
        resp_dict[item['page_date']['S']] = item_dict
    return resp_dict

def lambda_handler(event, context):
    logger.info("begin")
    date = event["queryStringParameters"]["date"]
    logger.info("Quering: {0}".format(date))
    # query = parse_date(date)
    response = query_db(date)
    formatted_response = format_response(response)
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
            'Access-Control-Allow-Methods': 'GET',
            },
        'body': json.dumps(formatted_response),
    }
