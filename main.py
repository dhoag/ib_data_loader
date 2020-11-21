import sys
import time
import argparse
from threading import Thread

import pandas as pd
from ibapi.contract import Contract
#from ib.ext.Contract import Contract

import datacol

def determine_expiry(trade_date: pd.Timestamp, date_roll=False):
    year = trade_date.year
    month = trade_date.month
    month_offset = 1 if date_roll else 0
    if month in (1,2,3):
        if month > (3 - month_offset) or (month == 3 and trade_date.day > 10):
            return str(year) + '06'
        return str(year) + '03'
    if month in (4,5,6):
            if month > (6 - month_offset) or (month == 6 and trade_date.day > 10):
                return str(year) + '09'
            return str(year) + '06'
    if month in (7,8,9):
        if month > (9 - month_offset) or (month == 9 and trade_date.day > 10):
            return str(year) + '12'
        return str(year) + '09'
    if month in (10,11,12):
        if month > (12 - month_offset) or (month == 12 and trade_date.day > 10):
            year = year +1
            return str(year) + '03'
        return str(year) + '12'

def main(args):
    symbol = args.symbol
    date = args.day
    file_name = symbol + '_' + date + '.bars'
    if args.thirty_min:
        file_name = symbol + '_30_min_' + date + '.bars'
    if args.two_hours:
        file_name = symbol + '_120_min_' + date + '.bars'
    if args.date_roll:
        file_name = file_name + 'T'

    contract = get_contract(symbol, date, args.date_roll)

    try:
        collector = datacol.Datacol()
        print('Collecting', date, 'data for', contract)
        with open(file_name, 'w') as outfile:
            thread = Thread(target=collector.run)
            thread.start()

            if args.thirty_min:
                data_list = thirty_min_bars(collector, contract, date, outfile)
            elif args.two_hours:
                data_list = two_hour_bars(collector, contract, date, outfile)
            else:
                data_list = five_sec_bars(collector, contract, date, outfile)

            collector.process(data_list)
            while len(data_list) > 0 or collector.request_in_flight == True:
                print(collector.count)
                time.sleep(5)
    except Exception as e:
        print('Problem collecting:', e)
        sys.exit(1)

    collector.drop_connection()
    print("All done.")


def get_contract(symbol:str, date, date_roll=False):
    contract = Contract()
    contract.symbol = symbol
    #contract.symbol = "ES"
    # contract.secType = 'CONTFUT'
    # contract.secType = 'FUT+CONTFUT'
    contract.secType = "FUT"
    contract.lastTradeDateOrContractMonth = determine_expiry(pd.to_datetime(date), date_roll)
    print(contract.lastTradeDateOrContractMonth)
    # contract.lastTradeDateOrContractMonth = "201803"
    contract.exchange = "GLOBEX"
    contract.currency = "USD"
    contract.includeExpired = 1
    return contract

def two_hour_bars(collector, contract, date, outfile)-> []:
    data_list = []
    collector.bar_size = '2 hours'
    data_list.append(collector.init(contract, date, '17:15:00', '60 D', outfile))
    return data_list

def thirty_min_bars(collector, contract, date, outfile) -> []:
    data_list = []
    collector.bar_size = '30 mins'
    data_list.append(collector.init(contract, date, '17:15:00', '60 D', outfile))
    return data_list

def five_sec_bars(collector, contract, date, outfile):
    d = pd.to_datetime(date) - pd.DateOffset(1)
    prev_date = '%04d%02d%02d' % (d.year, d.month, d.day)
    data_list = []
    for h in range(20, 24, 2):
        time_str = ('%02d:00:00' % h)
        data_list.append(collector.init(contract, prev_date, time_str, '7200 S', outfile))
    for h in range(0, 18, 2):
        time_str = ('%02d:00:00' % h)
        data_list.append(collector.init(contract, date, time_str, '7200 S', outfile))
    data_list.append(collector.init(contract, date, '17:15:00', '4500 S', outfile))
    return data_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load historical data for given symbol.')
    parser.add_argument('symbol', type=str, help='a symbol to collect')
    parser.add_argument('day', type=str, help='A day for which to collect')
    parser.add_argument('--30', dest='thirty_min', action='store_true')
    parser.add_argument('--120', dest='two_hours', action='store_true')
    parser.add_argument('--date_roll', action='store_true')

    args = parser.parse_args()
    print(args)
    main(args)
