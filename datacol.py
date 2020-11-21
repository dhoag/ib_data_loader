from ibapi.wrapper import EWrapper
from ibapi.common import *
from ibapi.client import EClient
from ibapi.utils import *
import time


class Datacol(EClient, EWrapper):
    def __init__(self):
        # In this example wrapper and client are the same instance
        EClient.__init__(self, self)
        self.nextValidOrderId = None
        self.permId2ord = {}
        self.tick_id = 0
        self.request_in_flight = False
        self.bar_size = '5 secs'
        self.what_to_show = 'TRADES'
        self.connect("127.0.0.1", 7497, 0)

    def _request_bars(self, contract, date, t, duration: str, outfile):
        self.outfile = outfile
        self.duration = duration
        self.tick_id =  self.tick_id + 1
        self.reqCurrentTime()
        time.sleep(1)
        end_datetime = ('%s %s EST' % (date, t))
        print (str(self.tick_id) + " " + end_datetime, self.bar_size, self.duration)
        #print(contract)
        self.reqHistoricalData(self.tick_id, contract, endDateTime=end_datetime,
                               durationStr=self.duration, barSizeSetting=self.bar_size, whatToShow=self.what_to_show,
                               useRTH=0, formatDate=1, keepUpToDate=False, chartOptions=[])
        self.request_in_flight = True
        self.count = 0

    # Create a lambda for the request that can put into an execution queue
    def init(self, contract, date, t, duration, outfile):
        return lambda : self._request_bars(contract, date, t, duration, outfile)

    ''' Process '''
    def process(self, request_list:[]=None ):
        ''' Single quotes '''
        if request_list is not None:
            self.request_list = request_list

        if self.request_list is not None and len(self.request_list) > 0:
            request = self.request_list.pop(0)
            time.sleep(20)
            self.current_request = request
            request()

    def drop_connection(self):
        self.disconnect()
        time.sleep(1)

    @iswrapper
    def error(self, id, code, msg:str ):
        #super().error(id, code, msg)
        if id == 1100:
            print("Broken")
        elif id == 1102:
            print("Restored")
            if self.request_in_flight is True:
                #Resend
                time.sleep(15)
                self.current_request()
        elif id not in (2104, 2106): #Market data connection
            print('[' + str(id) + '] error ' + msg)
            #-- Can't exit() as lots of non-errors come from here

    @iswrapper
    def winError(self, *args):
        super().error(*args)
        print(current_fn_name(), vars())

    @iswrapper
    def historicalDataEnd(self, reqId:int, start:str, end:str):
        print("Request completed " + start + " " + end)
        self.request_in_flight = False
        self.process()

    @iswrapper
    def historicalData(self, reqId: int, bar: BarData):
        if (self.count % 100) == 0:
            print("Iteration: " + str(self.count))
        self.count += 1
        print(bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.barCount,
              bar.average, False, file=self.outfile)
