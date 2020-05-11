#!/Users/dhoag/p372vert/bin/python

import argparse
from matplotlib import pyplot as plt
from mplfinance import plotting as mpf
import numpy as np
from matplotlib.dates import DateFormatter, SecondLocator, date2num
import pandas as pd

def main(args):
    es_bars = None
    for aFile in args.files: 
        es_bars2 = pd.read_csv(aFile, header=None, delim_whitespace=True)
        if es_bars is None: 
            es_bars = es_bars2
        else:
            es_bars = es_bars.append(es_bars2)

    es_bars.columns = [ 'date','timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'count', 'WAP', 'hasGaps']
    es_bars[['Open','High', 'Low', 'Close', 'Volume', 'count', 'WAP']] = es_bars[['Open','High', 'Low', 'Close', 'Volume', 'count', 'WAP']].apply(pd.to_numeric)
    es_bars['date_time'] = es_bars.date.apply(str) + ' ' +es_bars.timestamp
    es_bars.date_time = es_bars.date_time.apply(pd.to_datetime)
    plt.bar(es_bars['WAP'], es_bars['Volume'])
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze volume at WAP price levels')
    parser.add_argument('files', type=str, nargs='+', help='files to parse')

    args = parser.parse_args()
    main(args)
