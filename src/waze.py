from __future__ import print_function
import pickle
from urllib.parse import urlparse, parse_qs
import datetime
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

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
                RANGE_NAMES=['FormResponses!Q6:Q100', 'FormResponses!T6:T100']):
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

def parseRawWazeData(data):
    return_buffer = []
    x = [col['values'] for col in data['valueRanges']]
    url_list = [url[0] for url in x[0]]
    time_list = [time[0] for time in x[1]]
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


for event in WAZE_REGISTRY:
    x = getWazeData(SPREADSHEET_ID=event["spreadsheet_id"])
    x = parseRawWazeData(x)
    for i in x:
        print(",".join([i["lat"], i["lon"], i["time"], event["event"]]))
