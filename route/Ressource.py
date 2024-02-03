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
            if len(self.data) > 0 and 'resourceSets' in self.data:
                if len(self.data['resourceSets']) > 0 and 'resources' in self.data['resourceSets'][0]:
                    self.ressourceSet = self.data['resourceSets'][0]['resources'][0]
        except ValueError:
            self.err = {
                'flag': True,
                'value': ValueError("resourceSets not found in the api response")
            }

    # Attempt to set the ressourceSet from the data of the api bing map request
    def setCallbackUrl(self):
        try:
            if len(self.ressourceSet) > 0 and 'callbackUrl' in self.ressourceSet:
                self.callback_url = self.ressourceSet['callbackUrl']
            if len(self.ressourceSet) > 0 and 'requestId' in self.ressourceSet:
                self.requestId = self.ressourceSet['requestId']
        except ValueError:
            self.err = {
                'flag': True,
                'value': ValueError("resourceSets not found in the api response")
            }

    # Attempt to set the ressourceSet from the data of the api bing map request
    def setOrigins(self):
        try:
            if len(self.data) > 0 and 'origins' in self.data:
                self.origins = self.data['origins']
            if len(self.data) > 0 and 'results' in self.data:
                self.results = self.data['results']
        except ValueError:
            self.err = {
                'flag': True,
                'value': ValueError("resourceSets not found in the api response")
            }

    # Attempt to set the routePath from the ressourceSet bing map request
    def setRoutePath(self):
        if len(self.ressourceSet) > 0 and 'routePath' in self.ressourceSet:
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
        if len(self.ressourceSet) > 0 and 'routeLegs' in self.ressourceSet:
            self.routeLegs = self.ressourceSet["routeLegs"][0]

    # Set the coordinate of a giving address
    def setCoordinate(self):
        if len(self.ressourceSet) > 0 and 'point' in self.ressourceSet:
            if len(self.ressourceSet['point']) > 0 and 'coordinates' in self.ressourceSet['point']:
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
    