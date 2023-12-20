import alpaca_trade_api as api
import json as js
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
SCREENER_SNS_TOPIC_ORACULAR_SUBSCRIPTION_ARN = 'arn:aws:sns:us-east-1:232041705264:TradingScreenerTopic:4de70354-535d-491a-8b93-3c148f588a11'
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
      stocks = GetStocks()
      # Check limit and trade
      if len(stocks) > 0:
        CASH_FOR_TRADE_PER_SHARE = (float(account.non_marginable_buying_power) - CASH_LIMIT) / len(stocks)
        for item in stocks:
          predictions = item['predictions']
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

# Load data from Pub/Sub infrastructure - en principio no iría
# def LoadSub(sub_name):
#   data = []

#   subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
#   subscription_path = subscriber.subscription_path(PUBSUB_PROJECT_ID, sub_name)

#   def callback(message: pubsub_v1.subscriber.message.Message):
#       data.append(message)
#       message.ack()

#   streaming_pull_future = subscriber \
#     .subscribe(subscription_path, callback=callback)

#   with subscriber:
#       try:
#           streaming_pull_future.result(timeout=PUBSUB_TIMEOUT)
#       except TimeoutError:
#           streaming_pull_future.cancel()
#           streaming_pull_future.result()

#   return data

# Get stocks from Screener and Oracular
def GetStocks():
  stocks = []
  
  screener = LoadSub(PUBSUB_SCREENER_TOPIC_SUB_ID)
  oracular = LoadSub(PUBSUB_ORACULAR_TOPIC_SUB_ID)

  oracular_list = [x.attributes['stock'] for x in oracular]
  predictions = [x.attributes for x in oracular]
  pair_list = [(x.attributes, \
            [y for y in predictions if y['stock'] == x.attributes['stock']][0]) \
            for x in screener if x.attributes['stock'] in oracular_list]
  
  stocks = [{'stock': s[0]['stock'], \
           'operation': s[0]['operation'], \
           'stop_loss': float(s[0]['stop_loss']), \
           'take_profit': float(s[0]['take_profit']), \
           'shares_to_trade': int(s[0]['shares_to_trade']), \
           'predictions': [float(s[1]['day_1']),float(s[1]['day_2']),float(s[1]['day_3'])]} \
          for s in pair_list]

  return stocks

# TRADING
def Trade(api, stock, operation, shares_to_trade, take_profit, stop_loss):
  api.submit_order(symbol = stock, qty = shares_to_trade, side = operation, type = 'market',
                  order_class = 'bracket', time_in_force = 'gtc', 
                  take_profit = {'limit_price': take_profit},
                  stop_loss = {'stop_price': stop_loss})
  message = f'\n\t*{stock}*, qty _{shares_to_trade}_ \n\t\twere {operation}'
  send_message(f'{TRADER_BOT_NAME}: we entered the market with:' + message)
  return True


#Publicar predicciones a SNS topic
def PublishPredictions(stock, day_1, day_2, day_3):
   sns_client = boto3.client('sns', region_name=AWS_REGION)
   message = f'{stock}'
   sns_client.publish(
      TopicArn= ORACULAR_SNS_TOPIC_ARN,
      Message= message,
        MessageAttributes={
            'stock': {'DataType': 'String', 'StringValue': stock},
            'day_1': {'DataType': 'String', 'StringValue': str(day_1)},
            'day_2': {'DataType': 'String', 'StringValue': str(day_2)},
            'day_3': {'DataType': 'String', 'StringValue': str(day_3)},
        }
    )


  # Get stocks for work
  stocks = []
  for record in event['Records']:
    try:
      stock = record['Sns']['Message']
      stocks.append(stock)
    except Exception as e:
      print("An error occurred getting de stock from SNS message")
      raise e