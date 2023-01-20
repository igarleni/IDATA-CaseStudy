from urllib.request import Request
from urllib.request import urlopen
import boto3
import datetime
import os

TODAY_STR = datetime.datetime.today().strftime("%Y%m%d")
FILENAME = lambda x: f"{str(x)}_{TODAY_STR}.zip"


def lambda_handler(event, context):
    request = Request(os.environ['URL'])
    request.add_header('User-Agent',
                       'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36')
    response = urlopen(request)
    response_body = response.read()

    s3_client = boto3.client('s3')
    s3_client.put_object(Body=response_body, Bucket=os.environ['DataBucket'], Key=FILENAME("test"))

    event = dict()
    event['raw_zipfile_key'] = FILENAME("test")
    return event
