import json
from botocore.config import Config

class fileHandler:

    def __init__(self,logHandler):
        global log
        log = logHandler

    def loadconfig(self):
        with open('eventProcessing.config') as configfile:
            configData = json.load(configfile)
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
