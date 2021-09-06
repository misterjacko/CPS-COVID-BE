import boto3
import json
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

snsclient = boto3.client('sns')

def subscribe(email):
    updateARN = os.environ['snsTopicArn']

    # get topic name from env
    response = snsclient.subscribe(
        TopicArn = updateARN,
        Protocol = 'email',
        Endpoint = email,
    )
    return response



from datetime import datetime
cloudfrontclient = boto3.client('cloudfront')


def invalidateCache():
    response = cloudfrontclient.create_invalidation(
        DistributionId = 'E28928I27HS5YI',
        InvalidationBatch = {
            'Paths': {
                'Quantity': 1,
                'Items': [
                    '/data/*'
                ]
            },
            'CallerReference': str(datetime.now())
        }
    )
    return response




logger.info(invalidateCache())


def lambda_handler(event, context):
    email = event["queryStringParameters"]["email"]
    logger.info(subscribe(email))
    return {
        'statuscode': 200,
        'body': json.dumps('check email at {0} to complete subscription'.format(email))
    }
