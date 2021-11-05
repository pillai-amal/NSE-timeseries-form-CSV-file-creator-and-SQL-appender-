# NSE-timeseries-form-CSV-file-creator-and-SQL-appender-


This creates a ohlc timeseries from downloaded CSV files from National Stock Exchange India (NSE) website and makes a SQLite database for your research. 
This has made my work flow 95% faster compared to creating time series for individual tickers. Which even increases the time with increase in timeframe, earlier loops were used to go through entire csvs. I'm working for hosting the DB so that this is can be very much used like yfinance but for daily candles. 
Only daily infomation is available. As NSE publishes only daily inforation after the market hours. For intraday ohlc data I would suggest AlphaVantage which gives for Bombay Stock Exchange intraday values. In case if you trying to bulid a bot, there are brokers who give free data for using their APIs to place order. 

Feel free to contribute to this project.. 

Created table using the script 

![alt text](https://github.com/pillai-amal/NSE-timeseries-form-CSV-file-creator-and-SQL-appender-/blob/main/sample_table.png?raw=true "Logo Title Text 1")

Database Structure: 

![alt text](https://github.com/pillai-amal/NSE-timeseries-form-CSV-file-creator-and-SQL-appender-/blob/main/samle_db_Str.png?raw=true "Logo Title Text 1")
