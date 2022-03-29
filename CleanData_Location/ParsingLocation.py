

import json


files = ['linkedGeometry_California.json', 'linkedGeometry_Washington.json',
          'linkedGeometry_Florida.json', 'linkedGeometry_Georgia.json',
           'linkedGeometry_Michigan.json', 'linkedGeometry_Pennsylvania.json',
          'linkedGeometry_Texas.json', 'linkedGeometry_Colorado.json']


with open('parsed_Location.csv', "w+") as f:
    for file in files:
        print("Processing: " + file)
        state = file[15:-5]
        with open(file, "r") as f2:
            jdata = json.loads(f2.read())
            for entry in jdata:
                f.write(entry['GISJOIN'] + "," +str(state) + "," + str(entry['properties']['NAME10']) + "," + str(entry['properties']['NAMELSAD10']) + '\n')


