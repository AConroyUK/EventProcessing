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
            # Config(region_name = 'eu-west-1')
            #     signature_version = 'v4',
            #     retries = {
            #         'max_attempts': 5,
            #         'mode': 'standard'
            #     })
            log.info("Config loaded")
            return configData

    def loadjson(self,path):
        with open(path) as jsonfile:
            data = json.load(jsonfile)
            log.info(path + " loaded")
            return data
