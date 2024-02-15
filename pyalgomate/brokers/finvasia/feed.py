"""
.. moduleauthor:: Nagaraju Gunda
"""

import time
import datetime
import logging
import threading
import queue

from NorenRestApiPy.NorenApi import NorenApi

from pyalgotrade import bar
from pyalgomate.barfeed import BaseBarFeed

logger = logging.getLogger(__name__)

class SubscribeEvent(object):
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

    def __init__(self, eventDict):
        self.__eventDict = eventDict
        self.__datetime = None

    @property
    def exchange(self):
        return self.__eventDict["e"]

    @property
    def scriptToken(self):
        return self.__eventDict["tk"]

    @property
    def tradingSymbol(self):
        return self.__eventDict["ts"]

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
    def tickDateTime(self):
        return datetime.datetime.fromtimestamp(int(self.__eventDict['ft'])) if self.__eventDict.get(
            'ft', None) is not None else datetime.datetime.now()

    @property
    def price(self): return float(self.__eventDict.get('lp', 0))

    @property
    def volume(self): return float(self.__eventDict.get('v', 0))

    @property
    def openInterest(self): return float(self.__eventDict.get('oi', 0))

    @property
    def seq(self): return int(self.dateTime())

    @property
    def instrument(self): return f"{self.exchange}|{self.tradingSymbol}"

    def getBar(self):
        open = high = low = close = self.price

        return bar.BasicBar(self.dateTime,
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
                                "Date/Time": self.tickDateTime
                            })


class LiveTradeFeed(BaseBarFeed):
    def __init__(self, api, tokenMappings, timeout=10, maxLen=None):
        super(LiveTradeFeed, self).__init__(bar.Frequency.TRADE, maxLen)
        self.__api:NorenApi = api
        self.__tokenMappings = tokenMappings
        self.__timeout = timeout
        
        self.__lastDateTime = None
        self.__lastBars = dict()
        self.__tradeBars = queue.Queue()
        
        self.__wsThread = None
        self.__barEmitterThread = None
        self.__initialized = threading.Event()
        self.__stopped = False
        self.barEmitterFrequency = 0.5
        self.__pending_subscriptions = list()

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
        
        #super(LiveTradeFeed, self).start()
        self.__wsThread = threading.Thread(target=self.__startWebsocket)
        self.__wsThread.start()
        self.__barEmitterThread = threading.Thread(target=self.__barEmitter)
        self.__barEmitterThread.start()

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
        if self.__barEmitterThread is not None:
            self.__barEmitterThread.join()
            self.__barEmitterThread = None

    def eof(self):
        return self.__stopped
    
    def dispatch(self):
        pass

    def __initializeClient(self):
        initialized = False
        logger.info("Waiting for websocket initialization to complete")

        while not initialized and not self.__stopped:
            logger.info(f"Waiting for WebSocketClient waitInitialized with timeout of {self.__timeout}")
            initialized = self.__initialized.wait(self.__timeout)

        if initialized:
            logger.info("Initialization completed")
        else:
            logger.error("Initialization failed")

        return initialized
    
    def getNextValues(self):
        dateTime = None
        barsDict = dict()
        while self.__tradeBars.qsize() > 0:
            try:
                tradeBar:bar.BasicBar = self.__tradeBars.get_nowait()
                instrument = tradeBar.getExtraColumns().get("Instrument")

                if dateTime is None:
                    dateTime = tradeBar.getDateTime()
                    
                if tradeBar.getDateTime() != dateTime:
                    break

                barsDict[instrument] = tradeBar
            except Exception as e:
                pass
        
        if len(barsDict):
            return (dateTime, bar.Bars(barsDict))
        
        return (None, None)

    def __barEmitter(self):
        while not self.__stopped:
            dateTime, values = self.getNextValuesAndUpdateDS()
            if dateTime is not None:
                self.getNewValuesEvent().emit(dateTime, values)
                self.__lastDateTime = dateTime

            time.sleep(self.barEmitterFrequency)

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
    
    def __onSubscriptionSucceeded(self, event):
        logger.info(f"Subscription succeeded for <{event.exchange}|{event.tradingSymbol}>")

        self.__pending_subscriptions.remove(f"{event.exchange}|{event.scriptToken}")

        if not self.__pending_subscriptions:
            self.__initialized.set()
    
    def onTrade(self, tradeBar: bar.BasicBar):
        self.__tradeBars.put(tradeBar)
        instrument = tradeBar.getExtraColumns().get("Instrument")
        self.__lastBars[instrument] = tradeBar

    def onQuoteUpdate(self, message):
        logger.debug(message)

        field = message.get("t")
        message["ts"] = self.__tokenMappings[f"{message['e']}|{message['tk']}"].split('|')[
            1]
        # t='tk' is sent once on subscription for each instrument.
        # this will have all the fields with the most recent value thereon t='tf' is sent for fields that have changed.
        subscribeEvent = SubscribeEvent(message)

        if field not in ["tf", "tk"]:
            self.onUnknownEvent(subscribeEvent)
            return

        if field == "tk":
            self.__onSubscriptionSucceeded(subscribeEvent)

        subscribeEvent.dateTime = datetime.datetime.now().replace(microsecond=0)
        self.onTrade(subscribeEvent.getBar())
        
    def onOpened(self):
        logger.info("Websocket connected")
        self.__pending_subscriptions = list(self.__tokenMappings.keys())
        for channel in self.__pending_subscriptions:
            logger.info("Subscribing to channel %s." % channel)
            self.__api.subscribe(channel)
    
    def onClosed(self):
        logger.info("Websocket disconnected")

    def onError(self, exception):
        logger.error("Error: %s." % exception)
