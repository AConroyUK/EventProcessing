import json
import fileHandler
import boto3

class eventProcessing:

    def __init__(self):
        self.ACCESS_KEY, self.SECRET_KEY, self.SESSION_TOKEN = fileHandler.loadkeys()

    def main(self):

        client = boto3.client(
            's3',
            aws_access_key_id=self.ACCESS_KEY,
            aws_secret_access_key=self.SECRET_KEY,
            aws_session_token=self.SESSION_TOKEN,
            config=fileHandler.loadconfig()
        )


        s3 = boto3.resource('s3')
        for bucket in s3.buckets.all():
            print(bucket.name)
