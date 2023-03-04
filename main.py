import pandas as pd
import requests
import datetime

roman_numbers = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X']

headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9,id;q=0.8',
    'Connection': 'keep-alive',
    # 'Cookie': 'WSAntiforgeryCookie=CfDJ8FqGVPgzzoRMrgs4KxARAX9mXisKKfleLVgh6-xgOjKQ3t2YZT-0ZI4V0Lhlo_jhypm9rsHwk5w0-GnpBuLQejKSZYI-HSqOMtG51A08MH_l4yaRIlKhugaG_goz7To-LwC-UKnyj-VdkGogyy8bn_s; TS01a661ae=0199782b6fa078b77469eb7f43dc9613c82cd5a3278308aa7d841697bb0a50ba99fab9716690a1c28564faeb9bfe768e7fd2c2dd7e0f35d183cbcf336d9d621d970db77f62; _ga=GA1.3.860972258.1673831241; TS0dddebd2027=08f7caa0deab2000072d091af7a222b4d3f4475ea668e37bf36ba90027a716c33a7fcd198e8765a3080cd85d3b1130004e1e9695c7cce68deacc5044914069b7c12613f9e3464c921d220d8ae470a5cf0c4b689187dad455db6a7374617d7d9d; TS01441bdb=0199782b6fb87b73afb1eb2512a7bd5382ef765615d81e1a5c0c399b11a3a9eaf2115f14a4b3e9b3afa638fa27c98a599003f56b25',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.57',
    'X-Requested-With': 'XMLHttpRequest',
    'XSRF-TOKEN': 'CfDJ8FqGVPgzzoRMrgs4KxARAX-SOu1tJxuTKjnm6okujQip_XGIEzJe4waCGZfvtmHJrlD7YP4eK3CcJpfiXn_eeF1-cgyIATyw3yl9a395eO7o_W3zetLvvxmKS8W53Maxutc0TLj80cArXGfUVBuMI3I',
    'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Microsoft Edge";v="110"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

def extract(headers, start_date=datetime.date.today() - datetime.timedelta(days=7), end_date=datetime.date.today(), province_id=16, regency_id='', regency_name=None):

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

    return df

def transform(df):
    # Transform the dataframe structure to panel data
    df = pd.DataFrame(df.set_index(['name', 'regency_name']).stack()).reset_index().rename(columns={'level_2': 'date', 0: 'price'})

    # Change the data type for date and price columns
    df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
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

def main(headers, start_date=datetime.date.today() - datetime.timedelta(days=7), end_date=datetime.date.today(), province_id=16, regency_id=''):
    df = extract(headers=headers)
    df = transform(df)
    return df

df = main(headers=headers)
df.to_csv('test_df.csv', index=False)
