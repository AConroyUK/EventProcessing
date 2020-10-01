import json
import threading
import boto3
import datetime
import time
from multiprocessing import Process,Lock
from fileHandler import fileHandler
from logHandler import logHandler



class eventProcessing:
    def __init__(self):
        self.prev_messages = {}
        self.minute_messages = []
        self.batch_delete = []
        self.threads = []
        self.active_threads = 0
        self.queue_empty = False

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

    def calculate_average(self,file,minute_messages):
        if len(minute_messages)>0:
            file.csvoutput(sum(minute_messages)/len(minute_messages))
            log.info("output to csv")
        # datetime.utcfromtimestamp()
        # pass

    def pretty_print(self,location,event,value):
        string = "| " + str(location).rjust(36) + " | " + str(event).rjust(15) + " | " + str(value).rjust(20) + "|"
        print(string)
        #print("-"*66)

    def process_messages(self,resourcelock,locations,queue_url,sqs,thread_num):
        prev_messages = self.prev_messages
        minute_messages = self.minute_messages
        now = datetime.datetime.now()
        finish_time = now + datetime.timedelta(minutes = 1)
        log.info("thread "+str(thread_num)+" started")
        self.active_threads += 1
        while finish_time>now and len(self.threads) > thread_num:
            now = datetime.datetime.now()
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                VisibilityTimeout=20,
                WaitTimeSeconds=20
            )
            num_of_messages = len(response["Messages"])
            batch_delete = []
            for message in response["Messages"]:
                sensor = json.loads(message["Body"])
                batch_delete.append(dict([('Id',message["MessageId"]),('ReceiptHandle',message["ReceiptHandle"])]))
                #log.debug(sensor["Message"])
                try:
                    sensor_msg = json.loads(sensor["Message"])
                except json.decoder.JSONDecodeError:
                    try:
                        log.info("JSONDecodeError: "+str(sensor["Message"]))
                        break
                    except UnicodeEncodeError:
                        log.info("UnicodeEncodeError")
                        break

                if sensor_msg["locationId"] in locations:
                    resourcelock.acquire()
                    try:
                        try:
                            if sensor_msg["eventId"] in prev_messages[sensor_msg["locationId"]]:
                                #log.debug("duplicate event found")
                                log.debug("duplicate event: "+sensor_msg["eventId"])
                            else:
                                #log.debug(sensor_msg)
                                if len(prev_messages[sensor_msg["locationId"]]) == 25:
                                    prev_messages[sensor_msg["locationId"]].pop(0)
                                prev_messages[sensor_msg["locationId"]].append(sensor_msg["eventId"])
                                #self.pretty_print(sensor_msg["locationId"],sensor_msg["timestamp"],sensor_msg["value"])
                                minute_messages.append(int(sensor_msg["value"]))
                        except KeyError:
                            log.debug("location added: " + sensor_msg["locationId"])
                            prev_messages[sensor_msg["locationId"]] = []
                            #log.debug(sensor_msg)
                            prev_messages[sensor_msg["locationId"]].append(sensor_msg["eventId"])
                            #self.pretty_print(sensor_msg["locationId"],sensor_msg["timestamp"],sensor_msg["value"])
                            minute_messages.append(int(sensor_msg["value"]))
                    finally:
                        resourcelock.release()

            log.debug("processed "+str(num_of_messages)+" messages")
            self.prev_messages = prev_messages
            self.minute_messages = minute_messages
            self.batch_delete = batch_delete
        log.info("thread "+str(thread_num)+" stopped")
        self.active_threads -= 1


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
        # prev_messages = {}
        # minute_messages = []
        ticker = threading.Event()
        start_time = datetime.datetime.now()
        finish_time = start_time + datetime.timedelta(minutes = 1)
        now = datetime.datetime.now()
        last_average = now
        wait_time = 0.01
        resourcelock = Lock()
        self.threads = []
        response = []
        thread_num = len(self.threads)
        self.threads.append(threading.Thread(target=self.process_messages, args=(resourcelock,locations,queue_url,sqs,thread_num,),name="MsgProcessor"+str(thread_num)))
        self.threads[0].setDaemon(True)
        self.threads[0].start()
            # thread.join()

        while not ticker.wait(wait_time) and finish_time>now:
            now = datetime.datetime.now()

            if self.batch_delete != []:
                response = sqs.delete_message_batch(
                    QueueUrl=queue_url,
                    Entries=self.batch_delete
                )
                log.info("batch of " + str(len(self.batch_delete)) +" deleted")

            #prev_messages, minute_messages = self.process_messages(resourcelock,sqs,queue_url,locations,prev_messages,minute_messages)
            #self.process_messages(resourcelock,locations)

            response = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            num_of_messages = int(response["Attributes"]["ApproximateNumberOfMessages"])

            log.info("approx "+str(num_of_messages)+" messages in queue")
            file.message_num_output(num_of_messages,len(self.threads))
            if (10 < num_of_messages):
                if wait_time / 2 > 0.0001:
                    wait_time = wait_time / 2
                    log.info("reduced wait time to "+str(wait_time))
                elif len(self.threads) <= 30:
                    #create new thread
                    thread_num = len(self.threads)
                    self.threads.append(threading.Thread(target=self.process_messages, args=(resourcelock,locations,queue_url,sqs,thread_num,),name="MsgProcessor"+str(thread_num)))
                    self.threads[thread_num].setDaemon(True)
                    self.threads[thread_num].start()

            if (0 == num_of_messages):
                self.queue_empty = True
                if len(self.threads) > 1:
                    #delete thread
                    thread_num = len(self.threads) - 1
                    self.threads.remove(self.threads[thread_num])

                elif wait_time * 2 < 10:
                    wait_time = wait_time * 2
                    log.info("increased wait time to "+str(wait_time))


            if last_average + datetime.timedelta(minutes = 1) < now:
                last_average = now
                self.calculate_average(file,self.minute_messages)
                self.minute_messages = []
        log.info(self.threads)
        self.threads = []
        print(self.active_threads)
        if self.active_threads > 0:
            self.threads = []
            log.info("waiting for remaining threads to terminate")
        while self.active_threads > 0:
            time.sleep(0.1)

        response = sqs.delete_queue(QueueUrl=queue_url)
        log.info("Queue deleted")
        now = datetime.datetime.now()
        finish_time = now + datetime.timedelta(minutes = 1)
        while not ticker.wait(5) and finish_time>now:
            now = datetime.datetime.now()
        log.info("Ended")

if __name__ == "__main__":
    eventProcessing().main()
