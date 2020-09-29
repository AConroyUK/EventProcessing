import json
from fileHandler import fileHandler
from logHandler import logHandler
import boto3

class eventProcessing:

    def __init__(self):
        self.ACCESS_KEY = ""
        self.SECRET_KEY = ""
        self.BUCKET_NAME = "eventprocessing-altran-locationss3bucket-1ub1fsm0jlky7"

    def main(self):
        log = logHandler()
        file = fileHandler(log)
        log.info("Started")

        self.ACCESS_KEY, self.SECRET_KEY = file.loadkeys()

        s3 = boto3.client(
             's3',
             aws_access_key_id=self.ACCESS_KEY,
             aws_secret_access_key=self.SECRET_KEY,
             config=file.loadconfig()
        )

        s3.download_file(self.BUCKET_NAME, 'locations.json', 'locations.json')

        file.loadjson('locations.json')
        log.info("Ended")





if __name__ == "__main__":
    eventProcessing().main()
