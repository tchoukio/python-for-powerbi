import pandas as pd

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