import logging
import json
from botocore.config import Config


class fileHandler:

    def loadkeys(self):
          with open('aws.config') as configfile:
              configData = json.load(configfile)
              return configData['ACCESS_KEY'], configData['SECRET_KEY']

    def loadconfig(self):
        return Config(
            region_name = 'eu-west-2',
            signature_version = 'v4',
            retries = {
                'max_attempts': 5,
                'mode': 'standard'
            })
