import requests
import json
import time
from datetime import datetime
from Ressource import Ressource


#
#   Bing map api request class
#
class Api:
    def __init__(self, api_key):
        self.api_key = api_key
        self.domain = "https://dev.virtualearth.net/REST/v1/"
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
            url = f"{self.domain}Locations/{adress}?key={self.api_key}"
            response = requests.get(url)
            time.sleep(1)
            if response.status_code == 200:
                data = response.json()
        except ValueError:
            self.error = {
                'flag': True,
                'value': ValueError("data not found")
            }
        return Ressource(data)

    def getDataDistanceMatrixAsync(self, origins, destinations):
        data = {}
        try:
            start_time = self.getTimeFormat()
            travel_mode = "driving"
            route_type = "DistanceMatrixAsync"
            attribut = "routePath"
            endpoint = f"{self.domain}Routes/{route_type}?"

            api_nearest_url = f"{endpoint}travelMode={travel_mode}&origins={origins}&destinations={destinations}&startTime={start_time}&key={self.api_key}"
            nearest_response = requests.get(api_nearest_url)
            time.sleep(5)
            data = nearest_response.json()
        except ValueError:
            self.error = {
                'flag': True,
                'value': ValueError("data not found")
            }

        rs = Ressource(data)
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
            self.error = {
                'flag': True,
                'value': ValueError("data not found")
            }
        return Ressource(data)

    def getDataRoupePath(self, origin, destination):
        data = {}
        travel_mode = "Driving"
        attribut = "routePath"
        endpoint = f"{self.domain}Routes/{travel_mode}?"
        api_url = f"{endpoint}wp.0={origin}&wp.1={destination}&optmz=distance&routeAttributes={attribut}&key={self.api_key}"
        try:
            response = requests.get(api_url)
            time.sleep(1)
            data = response.json()
        except ValueError:
            self.error = {
                'flag': True,
                'value': ValueError("data not found")
            }
        return Ressource(data)