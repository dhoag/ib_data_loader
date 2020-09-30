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
    #Round to nearest dollar and group by price
    es_bars['wap_rnd'] = es_bars['WAP'].round(0)
    grouped_bars : pd.DataFrame = es_bars.groupby('wap_rnd').sum()
    #Shrink to just volume data
    grouped_bars = grouped_bars[['Volume']]
    counts : pd.Series = es_bars.groupby('wap_rnd').count()
    #Add counts at each price level
    grouped_bars['bar_count'] = counts['Close']
    #grouped_bars.to_csv('tmp.csv', header=True)

    print(grouped_bars[grouped_bars.Volume > np.percentile(grouped_bars.Volume, 95)])
    print(grouped_bars[grouped_bars.bar_count > np.percentile(grouped_bars.bar_count, 95)])

    #print(grouped_bars.describe())
    #print(counts.describe())

    #plt.bar(es_bars['WAP'], es_bars['Volume'])0:w

    #plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze volume at WAP price levels')
    parser.add_argument('files', type=str, nargs='+', help='files to parse')

    args = parser.parse_args()
    main(args)
