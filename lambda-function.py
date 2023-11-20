import json
import boto3
import urllib.parse
import csv
from io import StringIO

ses = boto3.client('ses', region_name='ap-south-1')
s3 = boto3.client('s3')

def lambda_handler(event, context):

    s3_bucket_name = event['Records'][0]['s3']['bucket']['name']
    s3_file_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    print(s3_bucket_name)
    print(s3_file_key)

    ses_sender_email = 'firstpirateking.onepiece@gmail.com'

    response = s3.get_object(Bucket=s3_bucket_name, Key=s3_file_key)
    csv_data = response['Body'].read().decode('utf-8')
    reader = csv.DictReader(csv_data.splitlines())
    for row in reader:
            recipient_email = row['Email']
            personalized_message = row['Message']

            # print(recipient_email)
            # print(personalized_message)

            send_email(ses, ses_sender_email, recipient_email, personalized_message)

def send_email(ses, sender_email, recipient_email, message):
    ses.send_email(
        Source = sender_email,
        Destination = {
            'ToAddresses': [recipient_email]
        },
        Message = {
            'Subject': {
                'Data': 'Testing SES',
                'Charset': 'UTF-8'
            },
            'Body': {
                'Text':{
                    'Data': message,
                    'Charset': 'UTF-8'
                }
            }
        }
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Successfully sent email from Lambda using Amazon SES')
    }
