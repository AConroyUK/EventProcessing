import json
import threading
import boto3
import datetime
import time
from queue import Queue
from multiprocessing import Process,Lock
from fileHandler import fileHandler
from logHandler import logHandler

class eventProcessing:
    def __init__(self):
        self.prev_messages = {}
        self.minute_messages = []
        self.location_data = {}
        self.threads = []
        self.message_queue = Queue()
        self._sentinel = object()

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

    def calculate_average(self,file,first_run):
        if len(self.minute_messages)>0:
            file.output_city_average(self.minute_messages,first_run)
            file.output_location_averages(self.location_data,first_run)
            log.info("output to csv")

    def pretty_print(self,location,event,value):
        string = "| " + str(location).rjust(36) + " | " + str(event).rjust(15) + " | " + str(value).rjust(20) + "|"
        print(string)
        #print("-"*66)

    def process_messages(self,resourcelock,locations,thread_num):
        now = datetime.datetime.now()
        log.info("thread "+str(thread_num)+" started")

        while True:
            try:
                message = self.message_queue.get()
                if message == self._sentinel:
                    break
                else:
                    sensor = json.loads(message["Body"])
                    try:
                        sensor_msg = json.loads(sensor["Message"])
                        keyError = False
                        try:
                            sensor_msg["locationId"]
                        except KeyError:
                            log.error("exception raised: KeyError")
                            keyError = True

                        if keyError == False and sensor_msg["locationId"] in locations:
                            resourcelock.acquire()
                            try:
                                if sensor_msg["eventId"] in self.prev_messages[sensor_msg["locationId"]]:
                                    log.debug("duplicate event: "+sensor_msg["eventId"])
                                else:
                                    if len(self.prev_messages[sensor_msg["locationId"]]) == 25:
                                        self.prev_messages[sensor_msg["locationId"]].pop(0)
                                    self.prev_messages[sensor_msg["locationId"]].append(sensor_msg["eventId"])
                                    self.minute_messages.append(int(sensor_msg["value"]))
                                    self.location_data[sensor_msg["locationId"]][0] += int(sensor_msg["value"])
                                    self.location_data[sensor_msg["locationId"]][1] += 1
                            except KeyError:
                                log.debug("location added: " + sensor_msg["locationId"])
                                self.prev_messages[sensor_msg["locationId"]] = []
                                self.prev_messages[sensor_msg["locationId"]].append(sensor_msg["eventId"])
                                self.minute_messages.append(int(sensor_msg["value"]))
                                self.location_data[sensor_msg["locationId"]] = []
                                self.location_data[sensor_msg["locationId"]].append(int(sensor_msg["value"]))
                                self.location_data[sensor_msg["locationId"]].append(int(1))

                            finally:
                                resourcelock.release()

                    except json.decoder.JSONDecodeError:
                        log.error("exception raised: json.decoder.JSONDecodeError")
                    except UnicodeEncodeError:
                        log.error("exception raised: UnicodeEncodeError")

            except Empty:
                pass

        log.info("thread "+str(thread_num)+" stopped")

        resourcelock.acquire()
        log.debug(self.threads)
        self.threads.remove(thread_num)
        log.debug(self.threads)
        resourcelock.release()


    def main(self):
        global log
        log = logHandler()
        file = fileHandler(log)
        resourcelock = Lock()
        ticker = threading.Event()
        MaxNumberOfMessages = 10
        first_run = True
        run_duration = 10

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

        now = datetime.datetime.now()
        finish_time = now + datetime.timedelta(minutes = run_duration)
        last_average = now

        thread_num = len(self.threads)
        self.threads.append(thread_num)
        new_thread = threading.Thread(target=self.process_messages, args=(resourcelock,locations,thread_num,),name="MsgProcessor"+str(thread_num))
        new_thread.setDaemon(True)
        new_thread.start()

        while finish_time>now:
            now = datetime.datetime.now()
            queue_empty = self.message_queue.empty()

            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=MaxNumberOfMessages,
                VisibilityTimeout=20,
                WaitTimeSeconds=20
            )
            messages = response["Messages"]
            log.debug("added to "+str(len(messages))+" message queue")

            if len(messages) == MaxNumberOfMessages:
                if len(self.threads) <= 30:
                    #add thread
                    resourcelock.acquire()
                    #thread_num = len(self.threads)
                    thread_num = 0
                    for thread in range(len(self.threads)+1):
                        if thread not in self.threads:
                            thread_num = thread
                            break

                    self.threads.append(thread_num)
                    resourcelock.release()
                    new_thread = threading.Thread(target=self.process_messages, args=(resourcelock,locations,thread_num,),name="MsgProcessor"+str(thread_num))
                    new_thread.setDaemon(True)
                    new_thread.start()
            elif queue_empty:
                if len(self.threads) > 1:
                    #terminate a thread
                    self.message_queue.put(self._sentinel)



            batch_delete = []
            for message in messages:
                self.message_queue.put(message)
                batch_delete.append(dict([('Id',message["MessageId"]),('ReceiptHandle',message["ReceiptHandle"])]))

            response = sqs.delete_message_batch(
                QueueUrl=queue_url,
                Entries=batch_delete
            )
            log.debug("batch of " + str(len(batch_delete)) +" deleted")

            file.message_num_output(len(batch_delete),len(self.threads),first_run)

            if last_average + datetime.timedelta(minutes = 1) < now:
                last_average = now
                self.calculate_average(file,first_run)
                first_run = False
                resourcelock.acquire()
                self.minute_messages = []
                resourcelock.release()

        for i in self.threads:
            self.message_queue.put(self._sentinel)

        if self.threads != []:
            log.info("waiting for "+str(len(self.threads))+" remaining threads to terminate")
            log.debug(self.message_queue)
            log.debug(self.threads)

        while self.threads != []:
            time.sleep(0.2)

        max = 0
        highest_toxicity = ""
        for locationId,data in self.location_data.items():
            average = 0
            if int(data[1]) > 0:
                average = float(data[0]) / int(data[1])
            if average > max:
                max = average
                highest_toxicity = locationId
        log.info("sensor with highest toxicity level("+str(max)+"): "+str(highest_toxicity))

        response = sqs.delete_queue(QueueUrl=queue_url)
        log.info("Queue deleted")
        now = datetime.datetime.now()
        finish_time = now + datetime.timedelta(minutes = 1)
        while not ticker.wait(5) and finish_time>now:
            now = datetime.datetime.now()
        log.info("Ended")

if __name__ == "__main__":
    eventProcessing().main()
