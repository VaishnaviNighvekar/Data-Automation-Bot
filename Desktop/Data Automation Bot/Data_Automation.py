# In[ ]:
from asyncio.windows_events import NULL
from typing import KeysView
from gspread.utils import rowcol_to_a1
from numpy.lib.function_base import insert
from numpy.lib.shape_base import column_stack
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os
import time
import gspread
import glob
import pickle
import csv
import json
import requests

tempf=NULL
while True:

    #import all values from the keys.xlsx sheet
    keys1 = pd.ExcelFile('keys.xlsx')
    keys=keys1.parse(0)

    #import data from the keys variable into corresponding variables
    download_path = keys['Download Folder Path'][0]
    excel_sheet = keys['spreadsheet id'][0]

    

    # Finding letest file in download folder
    #print('Finding Downloaded File')
    list_of_files = glob.iglob (download_path)
    latest_file = max(list_of_files, key=os.path.getctime)
    #print(latest_file)
    
    
    pattern="C:/Users/Vaishnavi nighvekar/Desktop/Data Automation Bot/Download/*"
    files = list(filter(os.path.isfile, glob.glob(pattern)))

    # sort by modified time
    files.sort(key=lambda x: os.path.getctime(x))

    # get last item in list

    lastfile = files[-1]
    lf=lastfile.split('\\')

    print("Data Transfer to cloud",lf[1])
    if (lf[1]!=".csv" and tempf!=lf[1]):
        
        tempf=lf[1]
        
        headers = {"Authorization": "Bearer ya29.a0Aa4xrXOUUUDC9bJ13IdyCEY7bdQqr50Tlp7dxYzvB1rG9wubUeHU2dxifyyi-iXL9fQLJ4nciOvwMU0XpN552H95WOCNwZatxOwqt4hjXaaID9Q5Q5SorRX2oxuUdUupzYHEHOkQb_nJSXL7OgBS0mEgSrbXaCgYKATASARMSFQEjDvL9TT6mpricwajcykX7cK7qsA0163"}
        para = {
            "name": lf[1],
        }
        files = {
            'data': ('metadata', json.dumps(para), 'application/json; charset=UTF-8'),
            'file': open(lastfile, "rb")
        }
        r = requests.post(
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
            headers=headers,
            files=files
        )
        # print(r.text)
        
        

    print(latest_file)
    print('File Found')

    file = open(latest_file, "r")
    csv_reader = csv.reader(file)

    lists_from_csv = []
    for row in csv_reader:
        lists_from_csv.append(row)
    # print(lists_from_csv)

    rows = lists_from_csv
    with open('IMP_1.csv', 'a', newline="") as f:
        write = csv.writer(f)
        write.writerows(rows)

    df = pd.read_csv('IMP_1.csv')
    govind = df.drop_duplicates(subset=['SR NO'], keep=False)
    # print(govind)


    govind.to_csv("IMP_2.csv", index=False, header=False)
    fi = open("IMP_2.csv", "r")
    csv_rea = csv.reader(fi)

    lists_fr = []
    for row in csv_rea:
        lists_fr.append(row)
    # print(lists_fr)

    
    print('Starting Data Copy in Google Sheet')

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
              "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    def main():

        # this is part of the url of google sheet
        spreadsheet_id = excel_sheet

        rows = lists_fr

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            'credentials.json')
        service = build('sheets', 'v4', credentials=credentials)
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range="Sheet1!A:Z",
            body={
                "majorDimension": "ROWS",
                "values": rows
            },
            valueInputOption="USER_ENTERED"
        ).execute()


    def get_or_create_credentials(scopes):
        credentials = None
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                credentials = pickle.load(token)
        if not credentials or not credentials.valid:
            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', scopes)
                credentials = flow.run_local_server(port=0)
            with open('token.pickle', 'wb') as token:
                pickle.dump(credentials, token)
        return credentials


    if __name__ == '__main__':
        main()

    print('Data written to google sheet successfully')
    print('Code run successfully... You may close this window')
    # driver.quit()


# %%
