"""
.. moduleauthor:: Nagaraju Gunda
"""

import datetime
import logging
from collections import defaultdict

from NorenRestApiPy.NorenApi import NorenApi

from pyalgotrade import bar
from pyalgomate.barfeed import BaseBarFeed
from pyalgomate.barfeed.BasicBarEx import BasicBarEx
from pyalgomate.brokers.finvasia import wsclient

logger = logging.getLogger(__name__)

class QuoteMessage(object):
    # t	tk	‘tk’ represents touchline acknowledgement
    # e	NSE, BSE, NFO ..	Exchange name
    # tk	22	Scrip Token
    # pp	2 for NSE, BSE & 4 for CDS USDINR	Price precision
    # ts		Trading Symbol
    # ti		Tick size
    # ls		Lot size
    # lp		LTP
    # pc		Percentage change
    # v		volume
    # o		Open price
    # h		High price
    # l		Low price
    # c		Close price
    # ap		Average trade price
    # oi		Open interest
    # poi		Previous day closing Open Interest
    # toi		Total open interest for underlying
    # bq1		Best Buy Quantity 1
    # bp1		Best Buy Price 1
    # sq1		Best Sell Quantity 1
    # sp1		Best Sell Price 1

    def __init__(self, eventDict, tokenMappings):
        self.__eventDict = eventDict
        self.__tokenMappings = tokenMappings

    def __str__(self):
        return f'{self.__eventDict}'

    @property
    def field(self):
        return self.__eventDict["t"]

    @property
    def exchange(self):
        return self.__eventDict["e"]

    @property
    def scriptToken(self):
        return self.__eventDict["tk"]

    @property
    def dateTime(self):
        #return datetime.datetime.fromtimestamp(int(self.__eventDict['ft']))
        return self.__eventDict["ct"]

    @property
    def price(self): return float(self.__eventDict.get('lp', 0))

    @property
    def volume(self): return float(self.__eventDict.get('v', 0))

    @property
    def openInterest(self): return float(self.__eventDict.get('oi', 0))

    @property
    def seq(self): return int(self.dateTime)

    @property
    def instrument(self): return f"{self.exchange}|{self.__tokenMappings[f'{self.exchange}|{self.scriptToken}'].split('|')[1]}"

    def getBar(self) -> BasicBarEx:
        open = high = low = close = self.price

        return BasicBarEx(self.dateTime,                
                    open,
                    high,
                    low,
                    close,
                    self.volume,
                    None,
                    bar.Frequency.TRADE,
                    {
                        "Instrument": self.instrument,
                        "Open Interest": self.openInterest,
                        "Message": self.__eventDict
                    }
                )


class LiveTradeFeed(BaseBarFeed):
    def __init__(self, api, tokenMappings, timeout=10, maxLen=None):
        super(LiveTradeFeed, self).__init__(bar.Frequency.TRADE, maxLen)
        self.__api:NorenApi = api
        self.__tokenMappings = tokenMappings
        self.__reverseTokenMappings = {value: key for key, value in tokenMappings.items()}
        self.__timeout = timeout
        
        for key, value in tokenMappings.items():
            self.registerDataSeries(value)
        
        self.__wsThread = None
        self.__stopped = False
        self.__lastDateTime = None

    def getCurrentDateTime(self):
        return datetime.datetime.now()

    def peekDateTime(self):
        # Return None since this is a realtime subject.
        return None

    def barsHaveAdjClose(self):
        return False

    def getNextBars(self):
        groupedQuoteMessages = defaultdict(dict)
        for lastBar in self.__wsThread.getQuotes().copy().values():
            quoteBar = QuoteMessage(lastBar, self.__tokenMappings).getBar()

            groupedQuoteMessages[quoteBar.getDateTime()][quoteBar.getInstrument()] = quoteBar

        latestDateTime = max(groupedQuoteMessages.keys(), default=None)
        bars = bar.Bars(groupedQuoteMessages[latestDateTime]) if latestDateTime is not None and self.__lastDateTime != latestDateTime else None
        self.__lastDateTime = latestDateTime
        return bars

    def getLastUpdatedDateTime(self):
        return max((QuoteMessage(lastBar, self.__tokenMappings).dateTime for lastBar in self.__wsThread.getQuotes().copy().values()), default=None)

    def isDataFeedAlive(self, heartBeatInterval=5):
        if self.getLastUpdatedDateTime() is None:
            return False

        currentDateTime = datetime.datetime.now()
        timeSinceLastDateTime = currentDateTime - self.getLastUpdatedDateTime()
        return timeSinceLastDateTime.total_seconds() <= heartBeatInterval

    def getApi(self):
        return self.__api
    
    def getLastBar(self, instrument) -> bar.Bar:
        lastBarQuote = self.__wsThread.getQuotes().get(self.__reverseTokenMappings[instrument], None)
        if lastBarQuote:
            return QuoteMessage(lastBarQuote, self.__tokenMappings).getBar()
        return None
    
    def __initializeClient(self):
        logger.info("Initializing websocket client")
        initialized = False
        try:
            # Start the thread that runs the client.
            self.__wsThread = wsclient.WebSocketClientThread(self.__api, self.__tokenMappings)
            self.__wsThread.start()
        except Exception as e:
            logger.error("Error connecting : %s" % str(e))

        logger.info("Waiting for websocket initialization to complete")
        while not initialized and not self.__stopped:
            initialized = self.__wsThread.waitInitialized(self.__timeout)

        if initialized:
            logger.info("Initialization completed")
        else:
            logger.error("Initialization failed")

        return initialized

    def start(self):
        if self.__wsThread is not None:
            logger.info("Already running!")
            return
        
        super(LiveTradeFeed, self).start()
        if not self.__initializeClient():
            self.__stopped = True
            raise Exception("Initialization failed")

    def stop(self):
        if self.__wsThread is not None:
            self.__api.close_websocket()
            self.__stopped = True
            self.join()
    
    def join(self):
        if self.__wsThread is not None:
            self.__wsThread.join()
            self.__wsThread = None

    def eof(self):
        return self.__stopped
    
    def dispatch(self):
        # Note that we may return True even if we didn't dispatch any Bar
        # event.
        ret = False
        if super(LiveTradeFeed, self).dispatch():
            ret = True
        return ret
