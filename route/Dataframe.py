import pandas as pd

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
        dataframe = {}
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
        return dataframe

    
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