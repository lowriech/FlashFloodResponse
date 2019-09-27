import geopandas as gpd
import requests
from zipfile36 import ZipFile
import gspread
from oauth2client.service_account import ServiceAccountCredentials



data_types = {
    "iowa_env_mesonet": {
        "base_url": "https://mesonet.agron.iastate.edu/pickup/wwa/{}"
    }
}

local_data_path = "../data/"

def get_data(source, remote_identifier, local_identifier):

    path = data_types[source]["base_url"].format(remote_identifier)
    out_zip = local_data_path + "{s}/{id}".format(s=source, id=remote_identifier)

    with open(out_zip, "wb") as file:
        r = requests.get(path)
        file.write(r.content)

    z = ZipFile(out_zip)
    z.extractall(local_data_path + source + "/" + local_identifier)



class DataSet:
    '''This is a wrapper based on geopandas.
    It takes a local path and constructs a GeoDataFrame,
    allowing for easy data manipulation.'''
    def __init__(self, shp_data_path):
        self.data = gpd.read_file(shp_data_path)

    def list_properties(self):
        return self.shp[0]["properties"].keys()

    def get_data_by_values(self, keys):
        x = self.data
        for key, value in keys.items():
            x = x.loc[x[key] == value]
        return x


class Waze:
    '''Keeping this as a separate class for now.
    Eventually will abstract DataSet more throughly, and override classes'''
    def __init__(self):
        # use creds to create a client to interact with the Google Drive API
        scope = ['https://spreadsheets.google.com/feeds']
        creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
        client = gspread.authorize(creds)

        # Find a workbook by name and open the first sheet
        # Make sure you use the right name here.
        sheet = client.open("Test-API").sheet1
        self.sheet = sheet

    def get_all_records(self):
        return self.sheet.get_all_records()




def test_iowa_env_mesonet():

    test_data = local_data_path + "iowa_env_mesonet/2019_tsmf_sbw/wwa_201901010000_201912312359.shp"
    x = DataSet(test_data)
    x.get_data_by_values({"PHENOM": "FF"})
    y = gpd.read_file(test_data)
    print(y.head())

