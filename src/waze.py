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
        "event": "Florence",
        "spreadsheet_id": "17LdrScaeiiZJPx8rc3in4QBf223ZaO8rDApRPK_20jc"
    },
    {
        "event": "WS Grayson",
        "spreadsheet_id": "1C-HUUUJmqW3sYpUazc5P_5i8yNkwLLPl72QVcL6wjMU"
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

def getWazeData(SPREADSHEET_ID = '1yeIgD2Dzb9TumUGLfixaVkdU_wU_lIB8_altNzVpxmY',
                RANGE_NAMES=['FormResponses!Q6:Q', 'FormResponses!T6:T']):
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
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
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()

    result = sheet.values().batchGet(spreadsheetId=SPREADSHEET_ID,
                                     ranges=RANGE_NAMES).execute()
    return result

def parseUrlList(unparsed_list):
    url_list = []
    for url in unparsed_list:
        if len(url) > 0:
            url_list.append(url[0])
        else:
            url_list.append("")
    return url_list

def parseTimeList(unparsed_list):
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
                    #t = time.mktime(time.strptime(timestring[0], p))
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

def parseRawWazeData(data):
    return_buffer = []
    x = [col['values'] for col in data['valueRanges']]
    url_list = parseUrlList(x[0])
    time_list = parseTimeList(x[1])
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

def fetchAllWaze(local_data_path):
    for event in WAZE_REGISTRY:
        with open(local_data_path + "waze/waze_" + event["event"] + ".txt", "w") as out_file:
            out_file.write(",".join(["lat", "lon", "time", "event"]) + "\n")
            x = getWazeData(SPREADSHEET_ID=event["spreadsheet_id"])
            x = parseRawWazeData(x)
            for i in x:
                out_file.write(",".join([i["lat"], i["lon"], i["time"], event["event"]])+"\n")

