"""
.. moduleauthor:: Nagaraju Gunda
"""

import time
import datetime
import logging
import threading
import zmq

from NorenRestApiPy.NorenApi import NorenApi

from pyalgotrade import bar
from pyalgomate.barfeed import BaseBarFeed
from pyalgomate.barfeed.BasicBarEx import BasicBarEx

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
        self.__datetime = None

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
        if self.__datetime is None:
            self.__datetime = datetime.datetime.fromtimestamp(int(self.__eventDict['ft'])) if self.__eventDict.get(
                'ft', None) is not None else datetime.datetime.now()

        return self.__datetime

    @dateTime.setter
    def dateTime(self, value):
        self.__datetime = value

    @property
    def price(self): return float(self.__eventDict.get('lp', 0))

    @property
    def volume(self): return float(self.__eventDict.get('v', 0))

    @property
    def openInterest(self): return float(self.__eventDict.get('oi', 0))

    @property
    def seq(self): return int(self.dateTime())

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
                        "Open Interest": self.openInterest
                    }
                )


class LiveTradeFeed(BaseBarFeed):
    def __init__(self, api, tokenMappings, timeout=10, maxLen=None):
        super(LiveTradeFeed, self).__init__(bar.Frequency.TRADE, maxLen)
        self.__api:NorenApi = api
        self.__tokenMappings = tokenMappings
        self.__timeout = timeout
        self.__connectionOpened = threading.Event()
        
        for key, value in tokenMappings.items():
            self.registerDataSeries(value)

        self.__lastDateTime = None
        self.__lastBars = dict()
        
        self.__wsThread = None
        self.__stopped = False
        self.__pending_subscriptions = list()

        self.__zmq_context = zmq.Context()
        self.__zmq_publisher = self.__zmq_context.socket(zmq.PUB)
        self.__zmq_publisher.bind(f"tcp://127.0.0.1:5555")

    def getCurrentDateTime(self):
        return datetime.datetime.now()

    def peekDateTime(self):
        # Return None since this is a realtime subject.
        return None

    def barsHaveAdjClose(self):
        return False

    def getNextBars(self):
        return None

    def getLastUpdatedDateTime(self):
        return self.__lastDateTime

    def isDataFeedAlive(self, heartBeatInterval=5):
        if self.__lastDateTime is None:
            return False

        currentDateTime = datetime.datetime.now()
        timeSinceLastDateTime = currentDateTime - self.__lastDateTime
        return timeSinceLastDateTime.total_seconds() <= heartBeatInterval

    def getApi(self):
        return self.__api
    
    def getLastBar(self, instrument) -> bar.Bar:
        return self.__lastBars.get(instrument, None)
    
    def start(self):
        if self.__wsThread is not None:
            logger.info("Already running!")
            return
        
        super(LiveTradeFeed, self).start()
        self.__wsThread = threading.Thread(target=self.__startWebsocket)
        self.__wsThread.start()

        logger.info('Waiting for connection to be opened')
        opened = self.__connectionOpened.wait(self.__timeout)

        if opened:
            logger.info('Connection opened. Waiting for subscriptions to complete')
        else:
            logger.error(f'Connection not opened in {self.__timeout} secs. Stopping the feed')
            self.stop()
            return

        for _ in range(self.__timeout):
            if {self.__tokenMappings[pendingSubscription]
                for pendingSubscription in self.__pending_subscriptions
                }.issubset(self.__lastBars.keys()):
                self.__pending_subscriptions.clear()
                logger.info("Initialization completed")
                break
            time.sleep(1)

    def stop(self):
        self.__zmq_publisher.close()
        self.__zmq_context.term()

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
        pass


    def __startWebsocket(self):
        self.__api.start_websocket(order_update_callback=self.onOrderBookUpdate,
                                   subscribe_callback=self.onQuoteUpdate,
                                   socket_open_callback=self.onOpened,
                                   socket_close_callback=self.onClosed,
                                   socket_error_callback=self.onError)


    def onOrderBookUpdate(self, message):
        pass

    def onUnknownEvent(self, event):
        logger.warning("Unknown event: %s." % event)

    def onQuoteUpdate(self, message):
        logger.debug(message)
        quoteMessage = QuoteMessage(message, self.__tokenMappings)
        tradeBar = quoteMessage.getBar()
        self.__lastBars[quoteMessage.instrument] = tradeBar
        self.__zmq_publisher.send_json(tradeBar.to_json())
        self.__lastDateTime = tradeBar.getDateTime()

    def onOpened(self):
        logger.info("Websocket connected")
        self.__pending_subscriptions = list(self.__tokenMappings.keys())
        for channel in self.__pending_subscriptions:
            logger.info(f"Subscribing to channel {channel} - Instrument {self.__tokenMappings[channel]}")
            self.__api.subscribe(channel)
        self.__connectionOpened.set()

    def onClosed(self):
        logger.info("Websocket disconnected")

    def onError(self, exception):
        logger.error("Error: %s." % exception)
