import pandas as pd
import numpy as np
import requests
import datetime
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

from utils import read_json, get_regency_ids, append_dataframe, read_config

roman_numbers = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X']

headers = read_json('headers.json')

# Parse command line arguments
parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("-s", "--start", default=datetime.date.today() - datetime.timedelta(days=7), help='Date with format "YYYY-MM-DD"')
parser.add_argument("-e", "--end", default=datetime.date.today(), help='Date with format "YYYY-MM-DD"')
parser.add_argument("-p", "--province", default=16, help='Input integer for province id')
parser.add_argument("-r", "--regency", default='', help='Input integer value for regency id')
args = vars(parser.parse_args())

# Set up parameters
start_date = args["start"]
end_date = args["end"]
province_id = args["province"]
regency_id = args['regency']

# Read configuration file
config = read_config('config.txt')
CREDENTIALS_PATH = config.get('config', 'CREDENTIALS_PATH')
SHEET_KEY = config.get('config', 'SHEET_KEY')
SHEET_NAME = config.get('config', 'SHEET_NAME')

def extract(headers, start_date=start_date, end_date=end_date, province_id=province_id, regency_id='', regency_name=None):

    params = {
    'price_type_id': '1',
    'comcat_id': '',
    'province_id': '{}'.format(province_id),
    'regency_id': '{}'.format(regency_id),
    'market_id': '',
    'tipe_laporan': '1',
    'start_date': '{}'.format(start_date),
    'end_date': '{}'.format(end_date),
    '_': '1677933857343',
    }

    response = requests.get(
    'https://www.bi.go.id/hargapangan/WebSite/TabelHarga/GetGridDataDaerah', params=params, headers=headers,)

    df = pd.DataFrame(response.json()['data'])
    df = df[~df.no.isin(roman_numbers)]
    df = df.drop(columns=['level', 'no'])
    df['regency_name'] = regency_name
    df.replace('-', np.nan)
    for column in df.columns:
        df[column] = np.where((df[column] == '-'), np.nan, df[column])

    return df

def transform(df):
    # Transform the dataframe structure to panel data
    df = pd.DataFrame(df.set_index(['name', 'regency_name']).stack()).reset_index().rename(columns={'level_2': 'date', 0: 'price'})

    # Change the data type for date and price columns
    df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
    df['date'] = df['date'].astype(str)
    df['price'] =  pd.to_numeric(df['price'].str.replace(',', ''))

    # Add category column
    def assign_category(row):
        name = row['name']
        if 'Beras' in name:
            category = 'Beras'
        else:
            category = ' '.join(name.split(' ')[0:2])
        return category

    df['category'] = df.apply(lambda row: assign_category(row), axis=1)

    return df

if __name__ == "__main__":
    regency_list = get_regency_ids(headers=headers, province_id=province_id)
    for i, regency in enumerate(regency_list):
        regency_id = regency['id']
        regency_name = regency['name']
        df = extract(headers=headers,
                     start_date=start_date,
                     end_date=end_date,
                     province_id=province_id,
                     regency_id=regency_id,
                     regency_name=regency_name)
        df = transform(df)
        append_dataframe(
            credentials_path=CREDENTIALS_PATH,
            sheet_key=SHEET_KEY,
            sheet_name=SHEET_NAME,
            data=df
        )
        print("Processed {} data".format(regency_name))
    
    
        
