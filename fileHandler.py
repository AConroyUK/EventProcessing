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

    def output_city_average(self,minute_messages,first_run):
        mean = sum(minute_messages)/len(minute_messages)
        if first_run:
            with open("city_average.csv", 'w') as csvfile:
                csvfile.write(str(mean)+"\n")
        else:
            with open("city_average.csv", "a+") as csvfile:
                csvfile.write(str(mean)+"\n")

    def output_location_averages(self,location_data,first_run):
        if first_run:
            with open("location_averages.csv", 'w') as csvfile:
                string = ""
                for locationId in location_data.keys():
                    string = string + (str(locationId)+",")
                csvfile.write(string+"\n")
                string = ""
                for data in location_data.values():
                    if int(data[1]) > 0:
                        average = int(data[0]) / int(data[1])
                        string = string + (str(average)+",")
                csvfile.write(string+"\n")
        else:
            with open("location_averages.csv", "a+") as csvfile:
                string = ""
                for data in location_data.values():
                    if int(data[1]) > 0:
                        average = int(data[0]) / int(data[1])
                        string = string + (str(average)+",")
                csvfile.write(string+"\n")


    def message_num_output(self,num_of_messages,num_of_threads):
        with open("num_of_messages.csv", "a+") as csvfile:
            csvfile.write(str(num_of_messages)+","+str(num_of_threads)+"\n")
