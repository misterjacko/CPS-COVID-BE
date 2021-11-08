import boto3
import json
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

snsclient = boto3.client('sns')

def subscribe(email):
    updateARN = os.environ['snsTopicArn']
    response = snsclient.subscribe(
        TopicArn = updateARN,
        Protocol = 'email',
        Endpoint = email,
        ReturnSubscriptionArn = True,
    )
    return response

def lambda_handler(event, context):
    email = event["queryStringParameters"]["email"]
    logger.info(email)
    logger.info(event)
    response = subscribe(email)
    logger.info("{0} subscribed to {1}".format(email, response))
    bodyString = 'Subscription initiated for {0}. You must click on the link in the email to confirm.'.format(email)
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
            'Access-Control-Allow-Methods': 'GET',
            },
        'body': json.dumps(bodyString),
    }
