"""
LOBSTER L3 Data Processing
Author: Mariol Jonuzaj
Date: 2025-11-20
"""

import numpy as np
import pandas as pd
import yaml
import os 

TICK_SIZE = 100
TRADING_START_SEC = int(9.5 * 3600)
ORDER_BOOK_LEVELS = 10

def read_data(root:str, folder: str, day:str, ticker:str) -> dict[str, pd.DataFrame]:
    try:
        filename_mes = f'{ticker}_{day}_34200000_57600000_message_10.csv'
        filename_ob = f'{ticker}_{day}_34200000_57600000_orderbook_10.csv'

        filepath_mes = root + folder + '/' + filename_mes
        filepath_ob = root + folder + '/' + filename_ob

        df_message = pd.read_csv(filepath_mes,header=None)
        df_ob = pd.read_csv(filepath_ob,header=None)

        return {'Message': df_message, 'OB': df_ob}
    except:
        print('An issue with imporing the {ticker} files!')



def get_data(df: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_message = df['Message']
    df_ob = df['OB']
    df_message.columns = ['Timestamp', 'Type', 'ID', 'Size', 'Price', 'Direction']
    
    levels = range(1, ORDER_BOOK_LEVELS+1) 
    ob_cols = [x 
           for level in levels
           for x in [f'Ask_Price_{level}',f'Ask_Size_{level}',
                     f'Bid_Price_{level}',f'Bid_Size_{level}']]
    df_ob.columns = ob_cols
    
    return df_message, df_ob
    
def merge_mes_ob(msg: pd.DataFrame, ob: pd.DataFrame) -> pd.DataFrame:
    df_merged = pd.concat([msg,ob],axis=1)
    return df_merged

def get_features(df: pd.DataFrame) -> pd.DataFrame:
    df['Midprice'] = (df['Ask_Price_1'] + df['Bid_Price_1']) / 2
    df['Spread'] = ((df['Ask_Price_1'] - df['Bid_Price_1']) / TICK_SIZE).astype(int)
    df['TBidSize'] = df[[f'Bid_Size_{i}' for i in range(1, ORDER_BOOK_LEVELS + 1)]].sum(axis=1)
    df['TAskSize'] = df[[f'Ask_Size_{i}' for i in range(1, ORDER_BOOK_LEVELS + 1)]].sum(axis=1)
    df['VolImb'] = (df['TBidSize'] - df['TAskSize']) / (df['TBidSize'] + df['TAskSize']) 
    return df

def agg_data_by_sec(df: pd.DataFrame, sec: int) -> pd.DataFrame:
    #Aggregate Data by {sec} seconds
    
    df[f'{sec}sec'] = ((df.Timestamp - TRADING_START_SEC) /sec) .astype(int)
    
    df_agg = df.groupby(f'{sec}sec').agg(
        Bid_Price_Open = ('Bid_Price_1', lambda x: x.iloc[0]),
        Bid_Price_Close = ('Bid_Price_1', lambda x: x.iloc[-1]),
        Ask_Price_Open = ('Bid_Price_1', lambda x: x.iloc[0]),
        Ask_Price_Close = ('Bid_Price_1', lambda x: x.iloc[-1]),
        Dmid_pct = ('Midprice', lambda x: x.iloc[-1] / x.iloc[0] - 1),
        Dmid_tick = ('Midprice', lambda x: (x.iloc[-1] - x.iloc[0])/TICK_SIZE),
        Spread_tick = ('Spread', 'mean'),
        VolImb = ('VolImb', 'mean'),
        Trades = ('Type', lambda x: (x==4).sum()),
        Volume=('Size', lambda x: x[df.loc[x.index, 'Type'] == 4].sum()),
        Buy_Trades=('Type', lambda x: ((x == 4) & (df.loc[x.index, 'Direction'] == -1)).sum()),
        Sell_Trades=('Type', lambda x: ((x == 4) & (df.loc[x.index, 'Direction'] == 1)).sum())
                  ).reset_index()

    df_agg['TFI'] = (df_agg['Buy_Trades'] - df_agg['Sell_Trades']) / (df_agg['Buy_Trades'] + df_agg['Sell_Trades'])
    return df_agg

def export_to_csv(df:pd.DataFrame, root:str, folder:str, ticker: str, interval_sec: int):
    filepath_to_export = root + folder + '/' + ticker + f'_{interval_sec}sec_agg.csv'
    try:
        df.to_csv(filepath_to_export,index=False)
        print(f'{ticker} agg data exported succesfully.')
    except:
        print(f'Error with exporting {ticker} agg data.')
    


if __name__ == "__main__":

    config_file = "config.yaml" if os.path.exists("config.yaml") else "public_config.yaml"

    with open(config_file) as f:
        config = yaml.safe_load(f)

    ROOT = config['ROOT']
    FOLDER = config['FOLDER_DATA']
    FOLDER_TO_SAVE = config['FOLDER_TO_SAVE']

    DAY = config['DAY']
    TICKER = config['TICKER']

    INTERVAL = [60,300]
    for value in INTERVAL:
        df = read_data(ROOT, FOLDER, DAY , TICKER)
        df_message, df_ob = get_data(df)
        df_merged = merge_mes_ob(df_message, df_ob)
        df_merged = get_features(df_merged)
        df_agg = agg_data_by_sec(df_merged, value)

        export_to_csv(df_agg, ROOT, FOLDER_TO_SAVE, TICKER, value)
