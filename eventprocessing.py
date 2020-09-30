import json
import threading
import boto3
from fileHandler import fileHandler
from logHandler import logHandler

class eventProcessing:

    def setup(self,access_key,secret_key,config,topic_arn):
        s3 = boto3.client(
             's3',
             aws_access_key_id=access_key,
             aws_secret_access_key=secret_key,
             config=config
        )
        sqs = boto3.client(
             'sqs',
             aws_access_key_id=access_key,
             aws_secret_access_key=secret_key,
             config=config
        )
        sns = boto3.client(
             'sns',
             aws_access_key_id=access_key,
             aws_secret_access_key=secret_key,
             config=config
        )

        response = sqs.create_queue(QueueName='sensors')
        log.info("Queue created")
        queue_url = response["QueueUrl"]
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['QueueArn']
        )
        queue_arn = response["Attributes"]["QueueArn"]
        log.debug("Queue url: "+queue_url)
        log.debug("Queue ARN: "+queue_arn)

        policy_document = {
            'Version': '2012-10-17',
            'Statement': [{
                'Sid': f'allow-subscription-{topic_arn}',
                'Effect': 'Allow',
                'Principal': {'AWS': '*'},
                'Action': 'SQS:SendMessage',
                'Resource': f'{queue_arn}',
                'Condition': {
                    'ArnEquals': { 'aws:SourceArn': f'{topic_arn}' }
                }
            }]
        }
        policy_json = json.dumps(policy_document)

        response = sqs.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={
                "Policy": policy_json
            }
        )

        response = sns.subscribe(
            TopicArn=topic_arn,
            Protocol='sqs',
            Endpoint=queue_arn
        )
        subscriptionArn = response["SubscriptionArn"]
        log.debug("Subscription ARN: "+subscriptionArn)

        return s3,sqs,queue_url,queue_arn

    def process_messages(self,sqs,queue_url,locations,prev_messages):
        log.debug("process messages")
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            VisibilityTimeout=20,
            WaitTimeSeconds=20
        )
        for message in response["Messages"]:
            sensor = json.loads(message["Body"])
            sensor_msg = json.loads(sensor["Message"])
            if sensor_msg["locationId"] in locations:
                try:
                    if sensor_msg["eventId"] in prev_messages[sensor_msg["locationId"]]:
                        #log.debug("duplicate event found")
                        log.debug("duplicate: "+str(sensor_msg))
                    else:
                        #log.debug(sensor_msg)
                        prev_messages[sensor_msg["locationId"]].append(sensor_msg["eventId"])
                        self.pretty_print(list(prev_messages.keys()).index(sensor_msg["locationId"]),list(prev_messages[sensor_msg["locationId"]]).index(sensor_msg["eventId"]),sensor_msg["value"])
                except KeyError:
                    prev_messages[sensor_msg["locationId"]] = []
                    #log.debug(sensor_msg)
                    prev_messages[sensor_msg["locationId"]].append(sensor_msg["eventId"])
                    self.pretty_print(list(prev_messages.keys()).index(sensor_msg["locationId"]),list(prev_messages[sensor_msg["locationId"]]).index(sensor_msg["eventId"]),sensor_msg["value"])

        return prev_messages

    def pretty_print(self,location,event,value):
        string = "| " + str(location).rjust(8) + " | " + str(event).rjust(5) + " | " + str(value).rjust(20) + "|"
        print(string)
        #print("-"*66)

    def main(self):
        global log
        log = logHandler()
        file = fileHandler(log)
        log.info("Started")
        config = file.loadconfig()
        access_key = config["ACCESS_KEY"]
        secret_key = config["SECRET_KEY"]
        bucket_name = config["BUCKET_NAME"]
        bucket_key = config["BUCKET_KEY"]
        topic_arn = config["SNS_TOPIC_ARN"]

        s3,sqs,queue_url,queue_arn = self.setup(
            access_key=access_key,
            secret_key=secret_key,
            config=config["AWS_CONFIG"],
            topic_arn=topic_arn
        )

        s3.download_file(bucket_name, bucket_key, 'locations.json')
        locations = file.loadjson('locations.json')

        ticker = threading.Event()
        test = 20
        print()
        print("| " + "Location".ljust(8) + " | " + "Event".ljust(5) + " | " + "Value".ljust(20) + "|")
        print("-"*42)
        prev_messages = {}
        while not ticker.wait(2) and test>0:
            test -= 1
            self.process_messages(sqs,queue_url,locations,prev_messages)


        response = sqs.delete_queue(QueueUrl=queue_url)
        log.info("Queue deleted")
        log.info("Ended")

if __name__ == "__main__":
    eventProcessing().main()
