import talib as tb
import yfinance as yf
import alpaca_trade_api as trade
import pandas as pd
import os
import config
import schedule
import time
import datetime


api = trade.REST(config.key, config.seckey, config.link, api_version='v2')
account = api.get_account()
positions = api.list_positions()


def on_data():
    with open('data_sets/company.csv') as j:
        companies = j.read().splitlines()
        for company in companies:
            symbol = company.split(',')[0]
            # mention period: #########################################################################################
            df = yf.download(symbol, period='1h', interval='1h')
            last = df.tail(2)  # y finance gives present time as output at last
            # drop the row with improper time frame
            last_value = last.head(1)  # get last two and get head
            last_value.to_csv(f'data_sets/1hour_timeframe/{symbol}.csv', mode='a', header=False)
            # df.to_csv(f'data_sets/1hour_timeframe/{symbol}.csv')

    j.close()


def read_data():
    data_files = os.listdir(f'data_sets/1hour_timeframe/')
    for file_name in data_files:
        # mention (df['Open'], df['High'], df['Low'], df['Close'])in csv
        df = pd.read_csv(f'data_sets/1hour_timeframe/{file_name}')
        try:
            macd, macdsignal, macdhist = tb.MACDEXT(df['Close'], fastperiod=3, fastmatype=0,
                                                    slowperiod=10,
                                                    slowmatype=0, signalperiod=16, signalmatype=0)
            macd = macd.iloc[-2:]
            macd_signal = macdsignal.iloc[-2:]
            macdsign_now = macd_signal.iloc[-1]
            macdsign_previous = macd_signal.iloc[0]
            macd_now = macd.iloc[-1]
            macd_previous = macd.iloc[0]

            if macd_now > macdsign_now and macd_previous < macdsign_previous and \
                    file_name.split('.')[0] not in positions:
                api.submit_order(
                    symbol=file_name.split('.')[0],
                    qty=1000,
                    side='buy',
                    type='market',
                    time_in_force='day',
                )
                # print(file_name.split('.')[0], ',buy')

            elif macd_now < macdsign_now and macd_previous > macdsign_previous and \
                    file_name.split('.')[0] in positions:
                api.submit_order(
                    symbol=file_name.split('.')[0],
                    qty=1000,
                    side='sell',
                    type='market',
                    time_in_force='day',
                )
                # print(file_name.split('.')[0], ',sell')
            else:
                pass
        except (OSError, Exception):
            # print(file_name)
            pass


def market_handler():
    schedule.every(1).hour.do(on_data, read_data)  # note sched only start at 9:30
    for i in range(8):
        schedule.run_pending()
        time.sleep(3600)


schedule.every().monday.at("08:30").do(market_handler)
schedule.every().tuesday.at("08:30").do(market_handler)
schedule.every().wednesday.at("08:30").do(market_handler)
schedule.every().thursday.at("08:30").do(market_handler)
schedule.every().friday.at("08:30").do(market_handler)

'''
RUN_LB = datetime.time(hour=8, minute=25)  # 8am
RUN_UB = datetime.time(hour=16)  # 16pm


def should_run():
    # Get the current time
    ct = datetime.datetime.now().time()
    # Compare current time to run bounds
    lbok = RUN_LB <= ct
    ubok = RUN_UB >= ct
    # If the bounds wrap the 24-hour day, use a different check logic
    if RUN_LB > RUN_UB:
        return lbok or ubok
    else:
        return lbok and ubok


# Helper function to determine how far from now RUN_LB is
def get_wait_secs():
    # Get the current datetime
    cd = datetime.datetime.now()
    # Create a datetime with *today's* RUN_LB
    ld = datetime.datetime.combine(datetime.date.today(), RUN_LB)
    # Create a timedelta for the time until *today's* RUN_LB
    td = ld - cd
    # Ignore td days (may be negative), return td.seconds (always positive)
    return td.seconds


while True:
    if should_run():
        schedule.run_pending()
    else:
        wait_secs = get_wait_secs()
        print(wait_secs)
        time.sleep(wait_secs)
'''