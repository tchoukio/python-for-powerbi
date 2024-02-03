import requests
import json
import time
from datetime import datetime

import pandas as pd

#
#   Bing map api request class
#
class Api:
    def __init__(self, api_key):
        self.api_key = api_key
        self.error = {
            'flag': False,
            'value': {}
        }

    def getTimeFormat(self):
        current_timestamp = int(datetime.now().timestamp())
        datetime_obj = datetime.fromtimestamp(current_timestamp)
        return datetime_obj.strftime('%Y-%m-%dT%H:%M:%S-00:00')

    def getDataOfAddress(self, adress):
        data = {}
        try:
            adresse_url = f"https://dev.virtualearth.net/REST/v1/Locations/{adress}?key={self.api_key}"
            adresse_response = requests.get(adresse_url)
            time.sleep(1)
            if adresse_response.status_code == 200:
                data = adresse_response.json()
        except ValueError:
            self.error.flag = True
            self.error.value = ValueError("data not found")
        return Ressource(data)

    def getDataDistanceMatrixAsync(self, origins, destinations):
        data = {}
        try:
            start_time = self.getTimeFormat()
            travel_mode = "driving"
            route_type = "DistanceMatrixAsync"
            attribut = "routePath"
            endpoint = f"http://dev.virtualearth.net/REST/V1/Routes/{route_type}?"

            api_nearest_url = f"{endpoint}travelMode={travel_mode}&origins={origins}&destinations={destinations}&startTime={start_time}&key={self.api_key}"
            nearest_response = requests.get(api_nearest_url)
            data = nearest_response.json()
        except ValueError:
            self.error.flag = True
            self.error.value = ValueError("data not found")

        rs = Ressource(data)
        time.sleep(5)
        return self.resolveDataRouteMatrix(rs.getRequestId())

    def resolveDataRouteMatrix(self, requestId):
        data = {}
        try:
            result_url = f"https://routematrixpremium.blob.core.windows.net/finalresults/{requestId}"
            result_response = requests.get(result_url)

            if result_response.status_code == 200:
                raw_data = result_response.content.decode("utf-8-sig")
                data = json.loads(raw_data)
        except ValueError:
            self.error.flag = True
            self.error.value = ValueError("data not found")
        return Ressource(data)

    def getDataRoupePath(self, origin, destination):
        data = {}
        travel_mode = "Driving"
        attribut = "routePath"
        endpoint = f"http://dev.virtualearth.net/REST/V1/Routes/{travel_mode}?"
        api_url = f"{endpoint}wp.0={origin}&wp.1={destination}&optmz=distance&routeAttributes={attribut}&key={self.api_key}"
        try:
            response = requests.get(api_url)
            time.sleep(1)
            data = response.json()
        except ValueError:
            self.error.flag = True
            self.error.value = ValueError("data not found")
        return Ressource(data)


class Dataframe:
    def __init__(self, dfHeader = []):
        self.dfHeader = dfHeader
        self.fireStationInCity = {}
        self.fireStationRoutepath = {}
        self.fireStationDf = {}
        self.err = {
            'flag': False,
            'value': {}
        }
        self.setDataframeHeader(dfHeader)
        
    # Set the header of the dataframe file
    def setDataframeHeader(self, dfHeader):
        self.dfHeader = dfHeader
        if len(self.dfHeader) < 1:
            self.dfHeader = ['id', 'latitude', 'longititude', 'label_id']
    
    def setFireStationRoutepath(self, origins, results):
        result = {}
        try:
            my_origins = pd.DataFrame(origins)
            my_results = pd.DataFrame(results)
            my_results.drop('departureTime', axis=1, inplace=True)
            my_origins['originIndex'] = my_origins.index

            self.fireStationRoutepath = pd.merge(my_origins, my_results, on='originIndex', how='left').sort_values(by='travelDistance')
            self.fireStationRoutepath['latitude'] = self.fireStationRoutepath['latitude'].apply(lambda x: '{:.6f}'.format(x)).astype(str)
            self.fireStationRoutepath['longitude'] = self.fireStationRoutepath['longitude'].apply(lambda x: '{:.6f}'.format(x)).astype(str)
            self.fireStationRoutepath.insert(0, 'merge', self.fireStationRoutepath['latitude'] + "," + self.fireStationRoutepath['longitude'])
        except:
            self.err = {
                'flag': True,
                'value': ValueError("routepath not found")
            }
        self.setFireStationDf()
    
    def setFireStationInCity(self, fsDataframe):
        self.fireStationInCity = fsDataframe
        self.setFireStationDf()

    def setFireStationDf(self):
        if len(self.fireStationInCity) > 0 and len(self.fireStationRoutepath) > 0:
            self.fireStationDf = pd.merge(self.fireStationInCity, self.fireStationRoutepath, on='merge', how='left')
            self.fireStationDf.drop(columns=['coord_y', 'coord_x', 'id_caserne', 'no_caserne', 'destinationIndex', 'originIndex'], axis=1, inplace=True)
            self.fireStationDf.sort_values(by='travelDistance', inplace=True)


    # get the header of the dataframe file
    def getDataframeHeader(self):
        return self.dfHeader
    
    # Get route path coordinate as a dictionnary
    def getCoordinatesAsDict(self, routePath):
        result = []
        if 'line' in routePath:
            if 'coordinates' in routePath['line']:
                lines = routePath['line']['coordinates']
                fields = self.getDataframeHeader()
                result = [{fields[0]: key, fields[1]: arr[0], fields[2]: arr[1], fields[3]: 1} for key, arr in enumerate(lines)]
        else:
            raise ValueError("Verified that the 'line' and 'coordinates' key exist in the routePath array")
        return result
    
    def getDataset(self, routePath):
        try:
            dict_list = self.getCoordinatesAsDict(routePath)
            dataframe = pd.DataFrame(dict_list)
            dataframe['id'] = dataframe['id'].astype(int)
            return dataframe
        except ValueError:
            self.err = {
                'flag': True,
                'value': ValueError("routepath not found")
            }

    
    def getFireStationRoutepath(self):
        return self.fireStationRoutepath
    
    def getFireStationInCity(self):
        return self.fireStationInCity
    
    def getFireStationDf(self):
        return self.fireStationDf

    def getNearestFireStationAddress(self):
        address = ''
        if len(self.fireStationDf) > 0 and 'adresse' in self.fireStationDf:
            address = self.fireStationDf['adresse'].iloc[0]
        return address

        # generate a csv file with specific data
    def getLabelDataframe(self, ressource):
        dataframe = []
        try:
            fields = ['id', 'distance', 'duration', 'duration_traffic']
            fields.append('origin')
            fields.append('destination')

            dict_list = [{
                fields[0]: 1,
                fields[1]: ressource.getTravelDistance(),
                fields[2]: ressource.getTravelDuration(),
                fields[3]: ressource.getTravelDurationTraffic(),
                fields[4]: ressource.getStartLocationName(),
                fields[5]: ressource.getEndLocationName()
            }]
       
            dataframe = pd.DataFrame(dict_list)
            dataframe['id'] = dataframe['id'].astype(int)
            dataframe['distance'] = dataframe['distance'].astype(float)
            dataframe['duration'] = dataframe['duration'].astype(int)
            dataframe['duration_traffic'] = dataframe['duration_traffic'].astype(int)
        except ValueError:
            self.err = {
                'flag': True,
                'value': ValueError("routepath not found")
            }
        return dataframe


class FireStation:
    def __init__(self, file_path):
        self.file_path = file_path
        self.dataframe = {}
        self.setDataframe()

    def setDataframe(self):
        self.dataframe = pd.read_csv(self.file_path)
    


    def getDataframe(self):
        return self.dataframe

    # Return list of fire station of a giving city
    def getFireStationOfCity(self, city):
        df = self.dataframe
        fsCity = df[df['nom_ssi'].str.contains(city, na=False)]
        fsCity.insert(0, 'merge', fsCity['coord_y'].apply(lambda x: '{:.6f}'.format(x)).astype(str) + "," + fsCity['coord_x'].apply(lambda x: '{:.6f}'.format(x)).astype(str))
        return fsCity
    
    def getCoordinate(self, city, asString = True):
        fs = self.getFireStationOfCity(city)
        return fs['merge'].str.cat(sep=';')

class Ressource:
    # data: ressource as result of the bing map API request
    def __init__(self, data):
        self.data = data
        self.ressourceSet = {}
        self.routePath = {}
        self.coordinates = {}
        self.routeLegs = {}
        self.coordinate = {}
        self.callback_url = ''
        self.requestId = ''
        self.origins = {}
        self.results = {}
        self.err = {
            'flag': False,
            'value': {}
        }
        self.setRessourceSet()
        self.setRoutePath()
        self.setCoordinates()
        self.setRouteLegs()
        self.setCoordinate()
        self.setCallbackUrl()
        self.setOrigins()

    # Attempt to set the ressourceSet from the data of the api bing map request
    def setRessourceSet(self):
        try:
            if 'resourceSets' in self.data:
                if 'resources' in self.data['resourceSets'][0]:
                    self.ressourceSet = self.data['resourceSets'][0]['resources'][0]
        except ValueError:
            self.err = {
                'flag': True,
                'value': ValueError("resourceSets not found in the api response")
            }

    # Attempt to set the ressourceSet from the data of the api bing map request
    def setCallbackUrl(self):
        try:
            if 'callbackUrl' in self.ressourceSet:
                self.callback_url = self.ressourceSet['callbackUrl']
            if 'requestId' in self.ressourceSet:
                self.requestId = self.ressourceSet['requestId']
        except ValueError:
            self.err = {
                'flag': True,
                'value': ValueError("resourceSets not found in the api response")
            }

    # Attempt to set the ressourceSet from the data of the api bing map request
    def setOrigins(self):
        try:
            if 'origins' in self.data:
                self.origins = self.data['origins']
            if 'results' in self.data:
                self.results = self.data['results']
        except ValueError:
            self.err = {
                'flag': True,
                'value': ValueError("resourceSets not found in the api response")
            }

    # Attempt to set the routePath from the ressourceSet bing map request
    def setRoutePath(self):
        if 'routePath' in self.ressourceSet:
            self.routePath = self.ressourceSet["routePath"]

    # Attempt to set the coordinates from the routePath bing map request
    def setCoordinates(self):
        if 'line' in self.routePath and 'coordinates' in self.routePath['line']:
            self.coordinates = self.routePath['line']['coordinates']
        else:
            self.err = {
                'flag': True,
                'value': ValueError("Verified that the 'line' and 'coordinates' key exist in the routePath array")
            }

    # get the route path ressourceSet coordinates for the map
    def setRouteLegs(self):
        if 'routeLegs' in self.ressourceSet:
            self.routeLegs = self.ressourceSet["routeLegs"][0]

    # Set the coordinate of a giving address
    def setCoordinate(self):
        if 'point' in self.ressourceSet and 'coordinates' in self.ressourceSet['point']:
            self.coordinate = self.ressourceSet['point']['coordinates']

    def getRessourceSet(self):
        return self.ressourceSet

    def getRoutePath(self):
        return self.routePath

    def getCoordinates(self):
        return self.coordinates
    
    def getRouteLegs(self):
        return self.routeLegs
    
    def getCallbackUrl(self):
        return self.callback_url

    def getRequestId(self):
        return self.requestId

    def getResults(self):
        return self.results

    def getOrigins(self):
        return self.origins

    # Get the coordinate of a giving address
    def getCoordinate(self, asString = True):
        coord = {}
        try:
            if asString and self.coordinate and self.coordinate[0] and self.coordinate[1]:
                coord = str(self.coordinate[0]) + "," + str(self.coordinate[1])
            elif 'point' in self.ressourceSet and 'coordinates' in self.ressourceSet['point']:
                coord = self.ressourceSet['point']['coordinates']
        except ValueError:
            self.err = {
                'flag': True,
                'value': ValueError("coordinate not found")
            }
        return coord

    # get the travel distance
    def getTravelDistance(self):
        travelDistance = 0
        if 'travelDistance' in self.ressourceSet:
            travelDistance = self.ressourceSet['travelDistance']
        else:
            self.error = {
                'flag': True,
                'value': ValueError("travelDistance not found")
            }
        return travelDistance

    # get the travel duration
    def getTravelDuration(self):
        travelDuration = 0
        if 'travelDuration' in self.ressourceSet:
            travelDuration = self.ressourceSet['travelDuration']
        else:
            self.error = {
                'flag': True,
                'value': ValueError("travelDuration not found")
            }
        return travelDuration

    # get the travel duration traffic
    def getTravelDurationTraffic(self):
        travelDurationTraffic = 0
        if 'travelDurationTraffic' in self.ressourceSet:
            travelDurationTraffic = self.ressourceSet['travelDurationTraffic']
        else:
            self.error = {
                'flag': True,
                'value': ValueError("travelDurationTraffic not found")
            }
        return travelDurationTraffic
    

    # Get destination adress
    def getEndLocationName(self):
        routeLegs = self.getRouteLegs()
        if 'endLocation' in routeLegs and 'name' in routeLegs['endLocation']:
            return routeLegs['endLocation']['name']
        return ''

    # Get the origin adress
    def getStartLocationName(self):
        routeLegs = self.getRouteLegs()
        if 'startLocation' in routeLegs and 'name' in routeLegs['startLocation']:
            return routeLegs['startLocation']['name']
        return ''
    



destination = '6056 rue portelance' # input("Enter the origin address:")
city = 'Laval' # input("Enter the City:")
api_key = "Agt9quyMt_H6DePqPbEiCv8YFJc1mzdE4DNsUJ7Ak2VUCWmCGOTdVyPLIGrDJ-tt"
# file_path = "C:/Users/tchoukio/Documents/powerbi/Casernes.csv"
file_path = "../Casernes.csv"

df = Dataframe()
api = Api(api_key)
fs = FireStation(file_path)

# Get the all address of fire station
df.setFireStationInCity(fs.getFireStationOfCity(city))

rsDest = api.getDataOfAddress(destination)
coodDest = rsDest.getCoordinate()
coordOrg = fs.getCoordinate(city)

rsMatrix = api.getDataDistanceMatrixAsync(coordOrg, coodDest)
df.setFireStationRoutepath(rsMatrix.getOrigins(), rsMatrix.getResults())

dfFireStation = df.getFireStationDf()

origin = df.getNearestFireStationAddress()

rsRoutepath = api.getDataRoupePath(origin, destination)

routepath = rsRoutepath.getCoordinates()
dfRoutepath = df.getDataset(rsRoutepath.getRoutePath())
dfLabel = df.getLabelDataframe(rsRoutepath)








