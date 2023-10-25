import pymongo
import pandas as pd
from flask import Flask, request, jsonify
import requests as re
import time as tt
import pyotp
from SmartApi import SmartConnect
import atexit  # Import the atexit module

app = Flask(__name__)

# MongoDB connection settings
MONGODB_URI = "mongodb+srv://usr:passwd@clusterx.qpk1zm7.mongodb.net/?retryWrites=true&w=majority"
DB_NAME = "testdb"
COLLECTION_NAME = "testcol2"

# Create a MongoDB client
client = pymongo.MongoClient(MONGODB_URI)
db = client[DB_NAME]

# Function to upload data to MongoDB
def upload_data_to_mongodb(data, collection):
    col = db[collection]
    for df in data:
        records = df.to_dict(orient='records')
        col.insert_many(records)

# Function to download data
def download_data():
    pd.set_option('display.max_columns', None)

    # Initialize symbol token map
    def init_symbol_token_map():
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        data = re.get(url).json()
        tk_df = pd.DataFrame.from_dict(data)
        tk_df['expiry'] = pd.to_datetime(tk_df['expiry'])
        tk_df = tk_df.astype({'strike': float})
        return tk_df

    expiri = "2023-10-26"

    def get_symbol_info(exch_seg, instrumenttype, symbol, strike_price, pe_ce):
        df = init_symbol_token_map()
        strike_price = strike_price * 100

        if exch_seg == 'NFO' and (instrumenttype == 'OPTSTK' or instrumenttype == 'OPTIDX'):
            return df[(df['exch_seg'] == exch_seg) & (df['instrumenttype'] == instrumenttype)
                      & (df['name'] == symbol) & (df['strike'] == strike_price)
                      & (df['symbol'].str.endswith(pe_ce) & (df['expiry'] == expiri))].sort_values(by='symbol')

    # Fold these functions into a single function
    def get_candle_data(symbolInfo, interval):
        try:
            historic_param = {
                "exchange": symbolInfo.exch_seg,
                "symboltoken": symbolInfo.token,
                "interval": interval,
                "fromdate": fromdate,
                "todate": todate
            }
            res_json = obj.getCandleData(historic_param)
            columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            df = pd.DataFrame(res_json['data'], columns=columns)
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%dT%H:%M:%S')
            df['symbol'] = symbolInfo.symbol
            df['expiry'] = symbolInfo.expiry
            print(f"Done for {symbolInfo.symbol} - {interval}")
            tt.sleep(0.2)
            return df
        except Exception as e:
            print(f"Historic Api failed: {e} {symbolInfo.symbol} - {interval}")

    fromdate = '2023-10-25 09:15'
    todate = '2023-10-25 15:30'

    api_key = 'nBgBs7Ku'
    username = 'P547740'
    password = '8894'
    totp_secret = 'T2PX34ABOGB3VZIVENMUAOJ5PQ'

    obj = SmartConnect(api_key=api_key)
    data = obj.generateSession(username, password, pyotp.TOTP(totp_secret).now())
    feedToken = obj.getfeedToken()

    init_symbol_token_map()
    
    # List of symbol info for which you want to download data
    symbol_infos = [
        get_symbol_info('NFO', 'OPTIDX', 'NIFTY', 19350, 'PE').iloc[0],
        get_symbol_info('NFO', 'OPTIDX', 'NIFTY', 19350, 'CE').iloc[0]
    ]
    
    # List of intervals for which you want to download data
    intervals = ["ONE_MINUTE", "THREE_MINUTE", "FIFTEEN_MINUTE", "THIRTY_MINUTE", "ONE_HOUR"]
    
    data_frames = []
    
    for symbol_info in symbol_infos:
        for interval in intervals:
            data_frames.append(get_candle_data(symbol_info, interval))
    
    return data_frames

@app.route('/api/upload-data', methods=['POST'])
def download_and_upload():
    # Download data
    downloaded_data = download_data()

    # Upload downloaded data to MongoDB
    upload_data_to_mongodb(downloaded_data, COLLECTION_NAME)

    return jsonify({"message": "Data downloaded and uploaded to MongoDB"})

@app.route('/')
# ‘/’ URL is bound with hello_world() function.
def hello_world():
    return 'Hello World'

if __name__ == '__main__':
    atexit.register(lambda: client.close())  # Close the MongoDB client when the application exits
    app.run()

