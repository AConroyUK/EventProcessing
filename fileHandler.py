import json
from botocore.config import Config

class fileHandler:

    def __init__(self,logHandler):
        global log
        log = logHandler

    def loadkeys(self):
          with open('aws.config') as configfile:
              configData = json.load(configfile)
              log.info("AWS keys loaded")
              return configData['ACCESS_KEY'], configData['SECRET_KEY']

    def loadconfig(self):
        log.info("Config loaded")
        return Config(region_name = 'eu-west-1')
            # signature_version = 'v4',
            # retries = {
            #     'max_attempts': 5,
            #     'mode': 'standard'
            # })

    def loadjson(self,path):
        with open(path) as jsonfile:
            data = json.load(jsonfile)
            for dict in data:
                x = str(dict["x"])
                y = str(dict["y"])
                id = str(dict["id"])
        log.info(path + " loaded")

        #         self.tHandler.readRow([date,fromAccount,toAccount,narrative,amount])
        # logging.info("Imported file:" + str(path))
        # self.tHandler.transactions.sort(key=self.sortKey)
