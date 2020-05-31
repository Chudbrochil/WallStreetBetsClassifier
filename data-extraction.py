import json
import datetime
import numpy as np
import pandas as pd

# Data date range:
# UTC+00:00
# 04/11/2012 09:46:43 AM 
# 10/31/2018 04:59:57 PM

def main():
    json_file = 'wsbData.json'
    output_file = 'wsb_data.pkl'
    cols = ['body', 'author', 'score', 'time']
    num_rows = 2979131

    # Open the file
    file = open(json_file, "r")

    # Extract all of the rows from the data and store it in a Pandas dataframe
    wsb_data = extract_data(file, cols, num_rows)

    # Saving off the data frame
    wsb_data.to_pickle(output_file)
    #check_pickle(output_file)

def extract_data(file, cols, num_rows):
    wsb_data = pd.DataFrame(index=range(num_rows), columns = cols)

    # Status check variable
    it = 0

    for line in file:
        data = json.loads(line)

        # 'ups', 'downs' are not in every file, we can't necessarily use them. We'll use 'score' instead.
        body = data['body']
        author = data['author']
        score = int(data['score'])
        raw_time = int(data['created_utc'])

        df_row = {'body' : body, 'author' : author, 'score' : score, 'time' : raw_time}
        wsb_data.iloc[it] = df_row

        if it % 10000 == 0:
            print(it)

        it += 1

    return wsb_data

def check_pickle(file):
    pickle = pd.read_pickle(file)
    print(pickle[:5])


# Used to get the earliest and latest timestamp's for our data.
def get_timestamps(file):

    earliest_time = float('inf')
    latest_time = float('-inf')

    for line in f:
        data = json.loads(line)

        raw_time = int(data['created_utc'])

        if raw_time < earliest_time:
            earliest_time = raw_time

        if raw_time > latest_time:
            latest_time = raw_time

    print(earliest_time)
    print(latest_time)
    print(datetime.datetime.fromtimestamp(earliest_time).strftime("%m/%d/%Y %I:%M:%S %p %Z"))
    print(datetime.datetime.fromtimestamp(latest_time).strftime("%m/%d/%Y %I:%M:%S %p %Z"))


if __name__ == "__main__":
    main()


