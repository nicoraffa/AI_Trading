import alpaca_trade_api as api
import yfinance as yf
import pandas_ta as ta
import json as js
import boto3


# SETTINGS
TRADER_API_KEY = 'PKUH3V70OHPE6CWU3F4L'
TRADER_API_SECRET = 'cswvwZ2CeLiYchIBQcwA4UUEy7Ph81BCsjoQ3lCe'
TRADER_API_URL = 'https://paper-api.alpaca.markets'

#PUBSUB_PROJECT_ID = '[PROJECT ID]'
#PUBSUB_TOPIC_ID = 'SharkScreenerTopic'

SCREENER_INTERVAL = '5m'
SCREENER_PERIOD = '250m'
SCREENER_NASDAQ_COUNT = 500

TA_RSI_TIMEPERIOD = 14
TA_BBANDS_LENGTH = 20
TA_BBANDS_STD = 2.3

TAKE_PROFIT_DELTA = 0.01
CASH_LIMIT = 26000

# secret = {
#   "type": "service_account",
#   "project_id": "[PROJECT ID]",
#   "private_key_id": "[PRIVATE KEY ID]",
#   "private_key": "-----BEGIN PRIVATE KEY-----\n[PRIVATE KEY]==\n-----END PRIVATE KEY-----\n",
#   "client_email": "[PROJECT_ID]@appspot.gserviceaccount.com",
#   "client_id": "[CLIENT_ID]",
#   "auth_uri": "https://accounts.google.com/o/oauth2/auth",
#   "token_uri": "https://oauth2.googleapis.com/token",
#   "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
#   "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/[PROJECT_ID]%40appspot.gserviceaccount.com"
# }

# service_account_info = js.loads(js.dumps(secret))
# credentials = service_account.Credentials. \
#               from_service_account_info(service_account_info)

# Check stock with TA indicators
def CheckStock(stock):
  data = {}
  try:
    df = yf.download(stock, period = SCREENER_PERIOD, interval = SCREENER_INTERVAL)
    if (len(df) > 0):
      df['RSI'] = ta.rsi(df['Close'], timeperiod=TA_RSI_TIMEPERIOD)
      bbands = ta.bbands(df['Close'], length = TA_BBANDS_LENGTH, std=TA_BBANDS_STD)
      df['L'] = bbands['BBL_20_2.3']
      df['M'] = bbands['BBM_20_2.3']
      df['U'] = bbands['BBU_20_2.3']
      
      previous2_bar = df[-3:].head(1)
      previous_bar = df[-2:].head(1)
      current_bar = df[-1:]

      if current_bar['RSI'].values[0] > 70 and \
          current_bar['Close'].values[0] > current_bar['U'].values[0]:
            data = { 'direction': 'DOWN', 'stock' : stock, \
                    'stop_loss': round(max(previous_bar['High'].values[0], previous2_bar['High'].values[0], previous_bar['U'].values[0]), 2), \
                    'take_profit': round(min(previous_bar['Low'].values[0], previous2_bar['Low'].values[0], previous_bar['M'].values[0]), 2) }
      elif current_bar['RSI'].values[0] < 30 and \
            current_bar['Close'].values[0] < current_bar['L'].values[0]:
              data = { 'direction': 'UP', 'stock' : stock, \
                      'stop_loss': round(min(previous_bar['Low'].values[0], previous2_bar['Low'].values[0], previous_bar['L'].values[0]), 2), \
                      'take_profit': round(max(previous_bar['High'].values[0], previous2_bar['High'].values[0], previous_bar['M'].values[0]), 2) }
  except:
    pass

  return data

# Screen stocks
def ScreenStocks(trader_api):
  assets = trader_api.list_assets(status='active', asset_class='us_equity')
  assets = [x for x in assets if x.shortable == True and x.exchange == 'NASDAQ']
  stocks = [x.symbol for x in assets][:SCREENER_NASDAQ_COUNT]

  screened = []
  for st in stocks:
    _stock = CheckStock(st)
    if _stock != {}:
      screened.append(_stock)

  screened = [x for x in screened if abs(x['stop_loss'] - x['take_profit']) > min(x['stop_loss'], x['take_profit']) * TAKE_PROFIT_DELTA]
  return screened

# Publish stock
def PublishStockToQueue(stock, operation, stop_loss, take_profit, shares_to_trade):
  publisher = pubsub_v1.PublisherClient(credentials=credentials)
  topic_path = publisher.topic_path(PUBSUB_PROJECT_ID, PUBSUB_TOPIC_ID)
  data_str = f'{stock}'
  data = data_str.encode("utf-8")
  publisher.publish(topic_path, \
                    data, \
                    stock=stock, \
                    operation=operation, \
                    stop_loss=f'{stop_loss}', \
                    take_profit=f'{take_profit}', \
                    shares_to_trade=f'{shares_to_trade}')

# Screener script
def shark_screener_go(request):
  trader_api = api.REST(TRADER_API_KEY, TRADER_API_SECRET, TRADER_API_URL)
  account = trader_api.get_account()
  screened = ScreenStocks(trader_api)
  screened = screened[0:3]
  if len(screened) > 0:
    CASH_FOR_TRADE_PER_SHARE = (float(account.non_marginable_buying_power) - CASH_LIMIT) / len(screened)
    for item in screened:
      STOCK = item['stock']
      OPERATION = 'buy' if item['direction'] == 'UP' else 'sell'
      STOP_LOSS = item['stop_loss']
      TAKE_PROFIT = item['take_profit']
      SHARE_PRICE = round(min(STOP_LOSS, TAKE_PROFIT), 2)
      SHARES_TO_TRADE = int(CASH_FOR_TRADE_PER_SHARE / SHARE_PRICE)
      try:
        if abs(STOP_LOSS - TAKE_PROFIT) > SHARE_PRICE * TAKE_PROFIT_DELTA and SHARES_TO_TRADE > 0:
          PublishStockToQueue(STOCK, OPERATION, STOP_LOSS, TAKE_PROFIT, SHARES_TO_TRADE)
          print(f'\n{STOCK} {OPERATION} {STOP_LOSS} {TAKE_PROFIT} {SHARES_TO_TRADE}')
      except:
        pass

  return f'Shark screener: DONE!'