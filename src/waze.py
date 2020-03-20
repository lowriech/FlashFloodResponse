from __future__ import print_function
import pickle
from urllib.parse import urlparse, parse_qs
import datetime
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import time
from datetime import datetime
from src.configuration import config
from src.spacetime.spacetime_handlers import AbstractGeoHandler, AbstractTimePointEvent
from src.spacetime.spacetime_analytics import SpaceTimePointStatistics
import pandas as pd
import geopandas as gpd

"""
This script parses raw Waze data from different prominent storms.
Waze data is supplied by the Waze VEOC in Google Sheets form.
It is not very internally consistent, which results in some decisions on how to parse it.
Most of the code follows Google's example API documentation:
- https://developers.google.com/sheets/api/quickstart/python"""

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

WAZE_REGISTRY = [
    {
        "event": "Dorian",
        "spreadsheet_id": "1tRaMYfLyqm2t6BjpylPU0jl2lYRBgMRaYOnrLipvgY4"
    },
    {
        "event": "Michael",
        "spreadsheet_id": "1G1-wS9kCs0v8XH4NeXdm8AswcgsRz1L9TK0zez39b3k"
    },
    {
        "event": "Florence",
        "spreadsheet_id": "17LdrScaeiiZJPx8rc3in4QBf223ZaO8rDApRPK_20jc"
    },
    {
        "event": "WS Grayson",
        "spreadsheet_id": "1C-HUUUJmqW3sYpUazc5P_5i8yNkwLLPl72QVcL6wjMU"
    },
    {
        "event": "Plains Flooding",
        "spreadsheet_id": "1TFElwO-4O-PrjtcfbJBY21WWUgrPU0GNgK3nIG4Qe18"
    },
    {
        "event": "Nate",
        "spreadsheet_id": "1C8OoVwJoBjCgV2CvOMOVDxixOp7mzxgrqsyq332r3m0"
    },
    {
        "event": "Maria",
        "spreadsheet_id": "1p0Y_2-IsFWCNqUS7cNLCZraxtsAX1alF8T3bD4QIItM"
    },
    {
        "event": "Irma",
        "spreadsheet_id": "1f62T9OCoszVe66pii-jD44T5FG-YwpCX6jiueewrU3U"
    },
    {
        "event": "Harvey",
        "spreadsheet_id": "1yeIgD2Dzb9TumUGLfixaVkdU_wU_lIB8_altNzVpxmY"
    }
]


def get_waze_from_google_sheets(spreadsheet_id='1yeIgD2Dzb9TumUGLfixaVkdU_wU_lIB8_altNzVpxmY',
                                range_names=('FormResponses!Q6:Q', 'FormResponses!T6:T')):
    """
    Fetch the Waze data from Google Sheets.
    This function follows closely with the example found here:
    - https://developers.google.com/sheets/api/quickstart/python
    """
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret.json', SCOPES)
            creds = flow.run_local_server(host='localhost',
                                          port=8080,
                                          authorization_prompt_message='Please visit this URL: {url}',
                                          success_message='The auth flow is complete; you may close this window.',
                                          open_browser=True)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().batchGet(spreadsheetId=spreadsheet_id,
                                     ranges=range_names).execute()
    return result


def parse_url_list(unparsed_list):
    """Parse URLs returned as a column from the Waze Sheets"""
    url_list = []
    for url in unparsed_list:
        if len(url) > 0:
            url_list.append(url[0])
        else:
            url_list.append("")
    return url_list


def parse_time_list(unparsed_list):
    """Parse the time column for information.
    Unfortunately the patterns aren't consistent, and currently we're handling that by checking for
    multiple time patterns."""
    time_list = []
    patterns = ['%m/%d/%Y %H:%M:%S',
                '%m/%d/%y %H:%M:%S',
                '%m/%d/%Y %H:%M',
                '%m/%d/%y %H:%M']
    for timestring in unparsed_list:
        if len(timestring) > 0:
            t = "NONE"
            for p in patterns:
                try:
                    t = time.strptime(timestring[0], p)
                    t = datetime.fromtimestamp(time.mktime(t))
                    break
                except:
                    continue
            t_year = t.year
            t_month = t.month
            t_day = t.day
            t_hour = t.hour
            t_minute = t.minute
            t_second = t.second
            t_list = ['{:02d}'.format(i) for i in [t_year, t_month, t_day, t_hour, t_minute, t_second]]
            t_string = "".join(t_list)
            time_list.append(t_string)
            if t == "NONE":
                raise ValueError

        else:
            time_list.append("0")
    return time_list


def parse_raw_waze_data(data):
    """
    Parse and format the raw values returned by Waze.
    Unfortunately the data is not well formatted.  The most consistent data comes from the URLs supplied,
    hence we create x,y points by parsing those URLs.
    """
    return_buffer = []
    x = [col['values'] for col in data['valueRanges']]
    url_list = parse_url_list(x[0])
    time_list = parse_time_list(x[1])
    intermediate_list = list(zip(url_list, time_list))

    for i in intermediate_list:
        parsed_url = urlparse(i[0])
        k = parse_qs(parsed_url.query)
        try:
            parsed = {
                "lat": k["lat"][0],
                "lon": k["lon"][0],
                "time": i[1]
            }
            return_buffer.append(parsed)
        except:
            pass
    return return_buffer


def fetch_all_waze_to_local(root):
    """Fetch all Waze VEOC sheets to local file system."""
    for event in WAZE_REGISTRY:
        file_name = event["event"] + ".txt"
        waze_path = os.path.join(root, file_name)
        with open(waze_path, "w") as out_file:
            out_file.write(",".join(["lat", "lon", "time", "event"]) + "\n")
            x = get_waze_from_google_sheets(spreadsheet_id=event["spreadsheet_id"])
            x = parse_raw_waze_data(x)
            for i in x:
                out_file.write(",".join([i["lat"], i["lon"], i["time"], event["event"]])+"\n")


class WazeHandler(AbstractGeoHandler, AbstractTimePointEvent, SpaceTimePointStatistics):

    t_field: str = "time"
    home_dir: str = config.waze

    def __init__(self, event_name):
        self.event_name = event_name
        AbstractGeoHandler.__init__(self, gdf=self.get_gdf())

    def get_gdf(self):
        """Get the Waze GDF from the .txt files pulled from Google Sheets"""
        from shapely.geometry import Point
        csv = os.path.join(self.home_dir, "waze_" + self.event_name + ".txt")
        df = pd.read_csv(csv)
        print(df)
        gdf = gpd.GeoDataFrame(
            df.drop(columns=['lon', 'lat']),
            crs={'init': 'epsg:4326'},
            geometry=[Point(xy) for xy in zip(df.lon, df.lat)]
        )
        gdf["time"] = gdf["time"]//100
        return gdf

    def prep_data(self):
        self.gdf = self.gdf[self.gdf[self.t_field] != 0]
        self.gdf[self.t_field] = self.gdf[self.t_field].apply(
            lambda t: self.convert_numeric_to_datetime(t)
        )

    @staticmethod
    def convert_numeric_to_datetime(time):
        time = str(time)
        return datetime(int(time[0:4]),
                        int(time[4:6]),
                        int(time[6:8]),
                        int(time[8:10]),
                        int(time[10:12]))


if __name__ == "__main__":
    fetch_all_waze_to_local(config.waze)
