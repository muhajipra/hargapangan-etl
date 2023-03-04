import requests
import json

import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from configparser import ConfigParser

def write_json(data, file_path):
    with open('{}.json'.format(file_path), 'w') as fp:
        json.dump(data, fp)

def read_json(file_path):
    with open(file_path) as json_data:
        data = json.load(json_data)
    return data

def read_config(config_path):
    configParser = ConfigParser()
    configFilePath = config_path
    configParser.read(configFilePath)
    return configParser

def get_regency_ids(headers, province_id=16):
    params_regency = {
    'price_type_id': '1',
    'ref_prov_id': '{}'.format(province_id),
    }

    response = requests.get(
        'https://www.bi.go.id/hargapangan/WebSite/TabelHarga/GetRefRegency',
        params=params_regency,
        headers=headers,
    )

    return response.json()['data']

def authenticate(credentials_path, sheet_key, sheet_name):
    scopes = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive',
    ]

    credentials = Credentials.from_service_account_file(credentials_path, scopes=scopes)

    gc = gspread.authorize(credentials)

    gauth = GoogleAuth()
    drive = GoogleDrive()

    gs = gc.open_by_key(sheet_key)
    worksheet = gs.worksheet(sheet_name)

    return gs, worksheet

def append_dataframe(credentials_path, sheet_key, sheet_name, data):
    gs, worksheet = authenticate(credentials_path=credentials_path, sheet_key=sheet_key, sheet_name=sheet_name)
    df_values = data.values.tolist()
    gs.values_append(sheet_name, {'valueInputOption': 'USER_ENTERED'}, {'values': df_values})



