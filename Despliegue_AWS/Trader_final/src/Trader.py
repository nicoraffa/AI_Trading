import alpaca_trade_api as api
import json
import requests as rq

# SETTINGS
TRADER_BOT_NAME = 'AI Medium Trader'

TRADER_API_KEY = 'PKUH3V70OHPE6CWU3F4L'
TRADER_API_SECRET = 'cswvwZ2CeLiYchIBQcwA4UUEy7Ph81BCsjoQ3lCe'
TRADER_API_URL = 'https://paper-api.alpaca.markets'

TELEGRAM_URL = 'https://api.telegram.org'
TELEGRAM_BOT_ID = 'bot6240899521:AAEjX6ayWtTb1FSU-OIZ8lbCoX7szwA3_jk'
TELEGRAM_CHAT_ID = '-1002121727585'


AWS_REGION = 'us-east-1'
SCREENER_SNS_TOPIC_ARN= 'arn:aws:sns:us-east-1:232041705264:TradingScreenerTopic'
ORACULAR_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:232041705264:TradingOracularTopic'
PUBSUB_TIMEOUT = 5.0

TAKE_PROFIT_DELTA = 0.02
CASH_LIMIT = 26000

# Trader script
def lambda_handler(event,context):
  trader_api = api.REST(TRADER_API_KEY, TRADER_API_SECRET, TRADER_API_URL)
  account = trader_api.get_account()
  clock = trader_api.get_clock()

  if bool(account) == True:
    message = f'''{TRADER_BOT_NAME}: for *{account.account_number}*
    current capital is _{account.portfolio_value}$_ 
    and non marginable buying power is _{account.non_marginable_buying_power}$_'''
    send_message(message)

  if clock.is_open == True:
    if float(account.non_marginable_buying_power) < CASH_LIMIT:
      message = f"{TRADER_BOT_NAME}: there is no cash on the account or limit reached!"
      send_message(message)
    else:
      stocks = GetStocks(event)
      # Check limit and trade
      if len(stocks) > 0:
        CASH_FOR_TRADE_PER_SHARE = (float(account.non_marginable_buying_power) - CASH_LIMIT) / len(stocks)
        for item in stocks:
          predictions = [float(item['day_1']),float(item['day_2']),float(item['day_3'])]
          STOCK = item['stock']
          OPERATION = item['operation']
          STOP_LOSS = min([item['stop_loss']] + predictions) if item['operation'] == 'buy' else max([item['stop_loss']] + predictions)
          TAKE_PROFIT = max([item['take_profit']] + predictions) if item['operation'] == 'buy' else min([item['take_profit']] + predictions)
          SHARE_PRICE = round(min(STOP_LOSS, TAKE_PROFIT), 2)
          SHARES_TO_TRADE = int(CASH_FOR_TRADE_PER_SHARE / SHARE_PRICE)
          try:
            if abs(STOP_LOSS - TAKE_PROFIT) > SHARE_PRICE * TAKE_PROFIT_DELTA and SHARES_TO_TRADE > 0:
              Trade(trader_api, STOCK, OPERATION, SHARES_TO_TRADE, TAKE_PROFIT, STOP_LOSS)
              print(f'\n{STOCK}: {STOP_LOSS}, {TAKE_PROFIT}, {OPERATION}, {SHARES_TO_TRADE}')
          except:
            pass

  portfolio = trader_api.list_positions()
  if bool(portfolio) == True:
    message = f'{TRADER_BOT_NAME}: we have {len(portfolio)} opened positions.'
    for i in portfolio:
      message = message + f'\n\t*{i.symbol}*: qty {i.qty} {i.side} for _{i.market_value}$_ \n\t\t\tcurrent price _{i.current_price}$_ \n\t\t\tprofit _{i.unrealized_pl}$_'
    send_message(message)
  
  if clock.is_open == False:
    message = f"{TRADER_BOT_NAME}: the market is *CLOSED*, let's try later on!"
    send_message(message)

  return f'{TRADER_BOT_NAME}: DONE!'

# Send message to Telegram channel
def send_message(message):
  response = rq.post(
        f'{TELEGRAM_URL}/{TELEGRAM_BOT_ID}/sendMessage?chat_id={TELEGRAM_CHAT_ID}&parse_mode=Markdown&text={message}')

  return response


# Get stocks from Screener and Oracular
def GetStocks(event):
  # Get stocks for work
  info_completa = []
  for record in event['Records']:
    try:
      message_data = json.loads(record['Sns']['Message'])
      for message_item in message_data:
         info_completa.append(message_item)
    except Exception as e:
      print("An error occurred getting stocks from SNS message")
      raise e
    
  return info_completa


# TRADING
def Trade(api, stock, operation, shares_to_trade, take_profit, stop_loss):
  api.submit_order(symbol = stock, qty = shares_to_trade, side = operation, type = 'market',
                  order_class = 'bracket', time_in_force = 'gtc', 
                  take_profit = {'limit_price': take_profit},
                  stop_loss = {'stop_price': stop_loss})
  message = f'\n\t*{stock}*, qty _{shares_to_trade}_ \n\t\twere {operation}'
  send_message(f'{TRADER_BOT_NAME}: we entered the market with:' + message)
  return True



