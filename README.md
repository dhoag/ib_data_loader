# ib_data_loader
A python script to download daily futures market data (5 sec bars) from Interactive Brokers using IbPy.

Usage: python main.py symbol date [30min]

symbol   - ES, NQ, CL, NG, GC, etc.

date     - YYYYMMDD format (e.g., 20150428)

Saves 5 sec bar data as a text file date.bars

The columns are:

timestamp, open, high, low, close, volume, count, WAP, hasGaps

loadmonth.sh 09 NQ


