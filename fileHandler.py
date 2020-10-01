import json
from botocore.config import Config

class fileHandler:

    def __init__(self,logHandler):
        global log
        log = logHandler

    def loadconfig(self):
        with open('eventProcessing.config') as configfile:
            configData = json.load(configfile)
            log.debug(configData)
            log.debug(configData[0])
            log.debug(configData[0]["CONFIG_TO_LOAD"])
            config_to_load = configData[0]["CONFIG_TO_LOAD"]
            configData = configData[config_to_load]
            configData["AWS_CONFIG"] = Config(region_name = 'eu-west-1')
            log.info("Config loaded")
            return configData

    def loadjson(self,path):
        locations = []
        with open(path) as jsonfile:
            data = json.load(jsonfile)
            for sensor in data:
                locations.append(sensor["id"])
            log.info(path + " loaded")
            return locations

    def csvoutput(self,mean):
        with open("eventProcessing.csv", "a+") as csvfile:
            csvfile.write(str(mean)+"\n")

    def message_num_output(self,num_of_messages,num_of_threads):
        with open("num_of_messages.csv", "a+") as csvfile:
            csvfile.write(str(num_of_messages)+","+str(num_of_threads)+"\n")
