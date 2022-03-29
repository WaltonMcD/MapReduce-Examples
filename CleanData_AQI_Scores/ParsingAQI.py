
import json


files = ['epa_aqi_general_score.California.raw_data.json', 'epa_aqi_general_score.Washington.raw_data.json',
          'epa_aqi_general_score.Florida.raw_data.json', 'epa_aqi_general_score.Georgia.raw_data.json',
           'epa_aqi_general_score.Michigan.raw_data.json', 'epa_aqi_general_score.Pennsylvania.raw_data.json',
          'epa_aqi_general_score.Texas.raw_data.json', 'epa_aqi_general_score.Colorado.raw_data.json']


with open('parsed_AQI.csv', "w+") as f:
    for file in files:
        print("Processing: " + file)
        with open(file, "r") as f2:
            jdata = json.loads(f2.read())
            for entry in jdata:
                f.write(entry['GISJOIN'] + "," + str(entry['aqi']) + "," + str(entry['epoch_time']['$numberLong']) + '\n')




