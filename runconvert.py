from pricedataprovider import PriceDataProvider
import json
import tqdm
import csv

def get_field_or_none(line_obj, field_name, make_int=False):
    if field_name in line_obj:
        if make_int:
            return int(line_obj[field_name])
        else:
            return line_obj[field_name]
    return None

wsbfile = open('wsbData.json')
outcsv = open("processed.csv", "w")
csvwriter = csv.writer(outcsv)
tickers = ["AMZN", "SPY", "TSLA", "AAPL"]
priceprovider = PriceDataProvider(tickers)
curr_line = 0
while True:
    line = wsbfile.readline()
    curr_line += 1
    if curr_line % 1000 == 0:
        print("Working on line {}".format(str(curr_line)))
    if not line:
        break
    line = line.strip()
    line_obj = json.loads(line)

    body = get_field_or_none(line_obj, "body")
    body = body.replace("\n", " ")
    author = get_field_or_none(line_obj, "author")
    ups = get_field_or_none(line_obj, "ups", make_int=True)
    downs = get_field_or_none(line_obj, "downs", make_int=True)
    score = get_field_or_none(line_obj, "score", make_int=True)
    post_time = get_field_or_none(line_obj, "created_utc", make_int=True)
    controversiality = get_field_or_none(line_obj, "controversiality", make_int=True)
    gilded = get_field_or_none(line_obj, "gilded", make_int=True)

    for ticker in tickers:
        if ticker in body:
            current_price = priceprovider.get_price(ticker, post_time)
            price_1d = priceprovider.get_future_price(ticker, post_time, 1)
            price_2d = priceprovider.get_future_price(ticker, post_time, 2)
            price_3d = priceprovider.get_future_price(ticker, post_time, 3)
            price_5d = priceprovider.get_future_price(ticker, post_time, 5)
            price_10d = priceprovider.get_future_price(ticker, post_time, 10)
            csvwriter.writerow([post_time, ticker, body, author, score, ups, downs, controversiality, gilded, current_price, price_1d, price_2d, price_3d, price_5d, price_10d])

outcsv.close()