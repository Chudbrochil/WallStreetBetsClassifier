import pandas as pd
from datetime import datetime, timedelta
import requests

api_key = "REDACTED"

# Actual earliest and latest times of our wsb data.
# 04/11/2012 09:46:43 AM 
# 10/31/2018 04:59:57 PM

# 4/1/2012 - 1/1/2019 (midnight), want to make sure we don't get out of bounds
# 1333238400 - 1546387199
# Generate Unix epoch beginning times for each day
def gather_data(api_key):
    begin_unix_time = 1333238400
    end_unix_time = 1546387200
    unix_times = range(begin_unix_time, end_unix_time, 60)

    datetimes = []
    initial_datetime = datetime(2012, 4, 1)
    end_datetime = datetime(2018, 12, 31)
    #end_datetime = datetime(2012, 6, 1)

    curr_datetime = initial_datetime
    while curr_datetime < end_datetime:
        datetimes.append(curr_datetime)
        curr_datetime = curr_datetime + timedelta(days=14)

    #tickers = ["TSLA", "SPY", "AAPL", "AMZN"]
    tickers = ["TSLA", "SPY", "AAPL", "AMZN", "AMD", "NVDA", "MSFT", "GOOG", "NFLX", "SBUX", "ADBE", "ORCL", "XOM", "WMT"]

    df = pd.DataFrame(index=unix_times, columns = tickers)

    # Need to process every 14 days. Request only retrieved 04-01-2012 - 04-19-2012 when requesting it all.
    prev_date_str = initial_datetime.strftime("%Y-%m-%d")
    for dt in datetimes[1:]:
        curr_date_str = dt.strftime("%Y-%m-%d")
        # Status update
        print(dt.strftime("%Y-%m-%d"))

        for ticker in tickers:

            URL = 'https://api.polygon.io/v2/aggs/ticker/' + ticker + '/range/1/minute/' + prev_date_str + '/' + curr_date_str + '?apiKey=' + api_key
            request = requests.get(url=URL)

            if request.ok == True:

                # Get the results and update the data frame with prices.
                request_text = request.json()['results']

                for tick in request_text:
                    unix_epoch_time = tick['t'] // 1000 # This time is in ms
                    price = tick['o'] # Using "open price"
                    df.loc[unix_epoch_time, ticker] = price

            else:
                print("Request failed on ticker: {0} at start date:{1} and end date:{2}".format(ticker, prev_date_str, curr_date_str))
                exit()

        prev_date_str = curr_date_str

        
    df.to_pickle('intraday_data_larger.pkl')

def check_pickle():
    import pandas as pd
    import random

    df = pd.read_pickle('intraday_data_larger.pkl')

    begin_unix_time = 1333238400
    end_unix_time = 1546387200
    #begin_unix_time = 1471046400
    #end_unix_time = 1471219200
    #begin_unix_time = 1471302000
    #end_unix_time = 1471330800
    unix_times = range(begin_unix_time, end_unix_time, 60)

    df = df.dropna(thresh=3)

    #sample = random.sample(unix_times, 10000)
    last_good_val = 0

    #for val in sample:
    for val in unix_times:
        if val in df.index:
        #if val > last_good_val:
        #    last_good_val = val
            print(df.loc[val, :])


check_pickle()

