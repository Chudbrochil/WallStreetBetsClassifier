import csv
import pandas as pd
import os
from datetime import datetime, time, timedelta
from dateutil import tz
import math


class PriceDataProvider:
    def __init__(self, ticker_list):
        self.ticker_list = ticker_list
        self.dataframes = {}
        self.intraday = pd.read_pickle('intraday_data.pkl')
        for ticker in self.ticker_list:
            self.dataframes[ticker] = pd.read_csv(ticker+".csv")
            self.dataframes[ticker].set_index('Date')

    def day_has_trades(self, ticker, timestamp):
        converted_time = self.timestamp_to_eastern(timestamp)
        curr_df = self.dataframes[ticker]
        date_str = converted_time.strftime('%Y-%m-%d')
        if len(curr_df.loc[curr_df['Date'] == date_str]) > 0:
            return True

        return False

    def is_time_between(self, begin_time, end_time, check_time=None):
        # If check time is not given, default to current UTC time
        check_time = check_time or datetime.utcnow().time()
        if begin_time < end_time:
            return check_time >= begin_time and check_time <= end_time
        else:  # crosses midnight
            return check_time >= begin_time or check_time <= end_time

    def is_during_trading_hours(self, ticker, timestamp):
        converted_time = self.timestamp_to_eastern(timestamp)
        return self.is_time_between(time(9, 30), time(16, 00), converted_time.time())

    def is_before_market_open(self, ticker, timestamp):
        converted_time = self.timestamp_to_eastern(timestamp)
        return self.is_time_between(time(00, 00), time(9, 29), converted_time.time())

    def is_after_market_close(self, ticker, timestamp):
        converted_time = self.timestamp_to_eastern(timestamp)
        return self.is_time_between(time(16, 1), time(23, 59), converted_time.time())

    def timestamp_to_eastern(self, timestamp):
        target_time = datetime.utcfromtimestamp(timestamp)
        from_zone = tz.gettz('UTC')
        to_zone = tz.gettz('America/New_York')
        target_time = target_time.replace(tzinfo=from_zone)
        eastern = target_time.astimezone(to_zone)
        return eastern

    def get_future_price(self, ticker, timestamp, days_ahead):
        days_ahead_remaining = days_ahead
        seconds_per_day = 86400
        if self.day_has_trades(ticker, timestamp) and self.is_before_market_open(ticker, timestamp):
            days_ahead_remaining -= 1
        
        while days_ahead_remaining > 0:
            timestamp += seconds_per_day
            if self.day_has_trades(ticker, timestamp):
                days_ahead_remaining -= 1
        
        curr_df = self.dataframes[ticker]
        eastern = self.timestamp_to_eastern(timestamp)
        date_str = eastern.strftime('%Y-%m-%d')

        row = curr_df.loc[curr_df['Date'] == date_str]
        return float(row['Close'])

    def get_price(self, ticker, timestamp):
        curr_df = self.dataframes[ticker]
        eastern = self.timestamp_to_eastern(timestamp)
        date_str = eastern.strftime('%Y-%m-%d')


        if self.day_has_trades(ticker, timestamp) and self.is_during_trading_hours(ticker, timestamp):

            rounded_timestamp = int(timestamp//60 * 60)
            price = self.intraday[ticker][rounded_timestamp]
            while math.isnan(price):
   
                rounded_timestamp -= 60
                price = self.intraday[ticker][rounded_timestamp]

            return price
            
        if self.day_has_trades(ticker, timestamp) and self.is_after_market_close(ticker, timestamp):
            # Post made after close, get closing price for the day
            row = curr_df.loc[curr_df['Date'] == date_str]
            return float(row['Close'])

        if (self.day_has_trades(ticker, timestamp) and self.is_before_market_open(ticker, timestamp)) or not self.day_has_trades(ticker, timestamp):
            # Not a trading day, or post was made before market open.
            # Search backward from previous day for latest available close price
            seconds_per_day = 86400
            days_to_subtract = 1
            new_timestamp = timestamp - days_to_subtract * seconds_per_day
            while not self.day_has_trades(ticker, new_timestamp):
                days_to_subtract += 1
                new_timestamp = timestamp - days_to_subtract * seconds_per_day
            new_date = self.timestamp_to_eastern(new_timestamp)
            new_date_str = new_date.strftime('%Y-%m-%d')
            row = curr_df.loc[curr_df['Date'] == new_date_str]
            return float(row['Close'])


if __name__ == "__main__":
    priceprovider = PriceDataProvider(["AMZN", "FB", "SPY", "TSLA"])
    print(priceprovider.get_future_price('AMZN', 1463146500, 2))

