import boto3

snsclient = boto3.client('sns')

def subscribe():
    # get topic name from env
    response = snsclient.subscribe(
        TopicArn='arn:aws:sns:us-east-1:480076109027:CPS-COVID-Dashboard-District-Updates',
        Protocol='email',
        Endpoint='ondrey@gmail.com',
    )
    return response

def lambda_handler(event, context):
    print(subscribe())
    return ('COMPLETE')

print(subscribe())