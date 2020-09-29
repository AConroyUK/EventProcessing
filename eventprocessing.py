import json
from fileHandler import fileHandler
import boto3

class eventProcessing:

    def __init__(self):
        self.ACCESS_KEY = ""
        self.SECRET_KEY = ""
        self.BUCKET_NAME = "eventprocessing-altran-locationss3bucket-1ub1fsm0jlky7"

    def main(self):
        fHandler = fileHandler()
        self.ACCESS_KEY, self.SECRET_KEY = fHandler.loadkeys()

        s3 = boto3.resource(
             's3',
             aws_access_key_id=self.ACCESS_KEY,
             aws_secret_access_key=self.SECRET_KEY,
             config=fHandler.loadconfig()
        )

        bucket = s3.Bucket(name=self.BUCKET_NAME)

        print(bucket.name)




if __name__ == "__main__":
    eventProcessing().main()
