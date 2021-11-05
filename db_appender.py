import pandas as pd
import sqlite3
import os
import datetime as dt

extension = '.csv'

#Delaration of lists for storing values of time series created form CSVs 
time_stamp_coll = []
open_price_coll = []
low_price_coll = []
high_price_coll = []
close_price_coll = []
qty_coll = []
date_list = []
ticker_list = []

#section of the lastes downloaded bhavcopy fom NSE website
sheet_name = str(len(os.listdir('C:/Users/pillai_amal/bhavcopy'))-1) + '.csv' #this assumes my NSE bhavcopy downloader is used and it automatically unzip the file and rename it as numbers, If you have a diffrent methond give the name of the latest file here 
latest_sheet = pd.read_csv(f"C:/Users/pillai_amal/bhavcopy/{sheet_name}")
con = sqlite3.connect('C:/Users/pillai_amal/NSE_AMAL/DataBase_From_Jan/nsedb.db') # give the name and location where you want to save the DB file
cur = con.cursor()

#here we ae considering only the ohlc information for the CSVs
latest_sheet_eq = latest_sheet.loc[latest_sheet['SERIES'] == 'EQ', ['SYMBOL','OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY', 'TIMESTAMP']]
latest_sheet_be = latest_sheet.loc[latest_sheet['SERIES'] == 'BE', ['SYMBOL','OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY', 'TIMESTAMP']]
latest_sheet = pd.concat([latest_sheet_eq, latest_sheet_be], axis=0)

#this functions makes a timeseries out of the ohlc data from entire csvs
def make_time_series(ticker):
    os.chdir(':/Users/pillai_amal/bhavcopy') #changing the directory to where the downloaded csvs are saved
    for bhavfile in os.listdir(): # selecting the entire CSVs in the directory
        if bhavfile.endswith(extension):
            data_file = pd.read_csv(bhavfile)
            # data_file_dropped = pd.read_csv(bhavfile)
            data_file_dropped = data_file.loc[data_file['SERIES'] == 'EQ', ['SYMBOL','OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY', 'TIMESTAMP']]
            data_file_dropped_with_be = data_file.loc[data_file['SERIES'] == 'BE', ['SYMBOL','OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY', 'TIMESTAMP']]
            data_file_dropped = pd.concat([data_file_dropped, data_file_dropped_with_be], axis=0)
            data_file_dropped.set_index('SYMBOL', inplace = True) 
            try:                                                                #appending the ohlc info into list for creating dataframe
                open_price_coll.append(data_file_dropped.at[ticker, 'OPEN'])
                low_price_coll.append(data_file_dropped.at[ticker, 'LOW'])
                high_price_coll.append(data_file_dropped.at[ticker, 'HIGH'])
                close_price_coll.append(data_file_dropped.at[ticker, 'CLOSE'])
                qty_coll.append(data_file_dropped.at[ticker, 'TOTTRDQTY'])
                time_stamp_coll.append(data_file_dropped.at[ticker, 'TIMESTAMP'])
            except KeyError:                                                    #there might be recent IPO where some information is not available there for appeding random info for creating dataframe, this can be removes using drop_duplicates in pandas 
                open_price_coll.append(0.1)
                low_price_coll.append(0.1)
                high_price_coll.append(0.1)
                close_price_coll.append(0.1)
                qty_coll.append(0.1)
                time_stamp_coll.append("2021/01/01")
                print(f"{ticker} has some error in file {bhavfile}") #just for notification that that particular day the selected ticker was not up for t4rading for was not listed until now (new IPO)
        gen_df = pd.DataFrame({'date' : time_stamp_coll, 'open' : open_price_coll,'high' : high_price_coll, 'low' : low_price_coll, 'close' : close_price_coll, 'volume' : qty_coll })
        gen_df['date']= pd.to_datetime(gen_df['date']).dt.date #convert to datetime
        gen_df.set_index('date', inplace= True)
        gen_df.sort_index(inplace= True)
    # all this list miust be cleared to avoid incorrecting carrying info from one ticker to another
    time_stamp_coll.clear()
    open_price_coll.clear()
    low_price_coll.clear()
    high_price_coll.clear()
    close_price_coll.clear()
    qty_coll.clear()
    return(gen_df) # the function returns the gnerated dataframe


#this is the function which aoends the ohlc information to the SQlite database ot createds a time series table if a table for such a table does not exist
def db_append():
    for indx in latest_sheet.index:
        cur.execute(f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name = '{latest_sheet['SYMBOL'][indx]}'") #checking if such a table exsist already
        if cur.fetchone()[0] ==1:
            try: #table is already present, therefor appening the latest ohlc inforation 
                cur.execute(f"INSERT INTO '{latest_sheet['SYMBOL'][indx]}' VALUES ('{latest_sheet['TIMESTAMP'][indx]}','{latest_sheet['OPEN'][indx]}', '{latest_sheet['HIGH'][indx]}', '{latest_sheet['LOW'][indx]}', '{latest_sheet['CLOSE'][indx]}', '{latest_sheet['TOTTRDQTY'][indx]}')")
                con.commit()
            except KeyError: #every rare care should occur, just to apped to catch some error 
                print(f"{latest_sheet['SYMBOL'][indx]} is not present in one of the csvs")
        else: #table for the selected ticker is not present, we sill be creating a new table fot timesieres data
            try:
                cur.execute((f" CREATE TABLE '{latest_sheet['SYMBOL'][indx]}' (date TEXT, open DECIMAL, high DECIMAL, low DECIMAL, close DECIMAL, volume DECIMAL);"))
                con.commit()
                df = make_time_series(latest_sheet['SYMBOL'][indx])
                df.drop_duplicates()
                print(df)
                for ind in df.index:
                    cur.execute(f"INSERT INTO '{latest_sheet['SYMBOL'][indx]}' VALUES ('{ind}','{df['open'][ind]}','{df['high'][ind]}','{df['low'][ind]}','{df['close'][ind]}','{df['volume'][ind]}');")
                    con.commit()
            except KeyError: 
                print(f"{latest_sheet['SYMBOL'][indx]} is not present in one of the csvs")


if __name__ == '__main__':
    db_append()


#for a any doubts or assistance conat me at amalmpillai7@gmail.com