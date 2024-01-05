import alpaca_trade_api as tradeapi
import yfinance as yf
import pandas_ta as ta
import json
import random
import boto3


# SETTINGS
TRADER_API_KEY = 'PKUH3V70OHPE6CWU3F4L'
TRADER_API_SECRET = 'cswvwZ2CeLiYchIBQcwA4UUEy7Ph81BCsjoQ3lCe'
TRADER_API_URL = 'https://paper-api.alpaca.markets'

PUBSUB_TOPIC_ARN = 'arn:aws:sns:us-east-1:232041705264:TradingScreenerTopic'

SCREENER_INTERVAL = '5m'
SCREENER_PERIOD = '250m'
SCREENER_NASDAQ_COUNT = 500

TA_RSI_TIMEPERIOD = 14
TA_BBANDS_LENGTH = 20
TA_BBANDS_STD = 2.3

TAKE_PROFIT_DELTA = 0.01
CASH_LIMIT = 26000

# Screener script
def lambda_handler(event, context):
    trader_api = tradeapi.REST(TRADER_API_KEY, TRADER_API_SECRET, TRADER_API_URL)
    account = trader_api.get_account()
    screened = screen_stocks(trader_api)
    random.shuffle(screened)
    screened = screened[0:3]
    mensajes = []
    if len(screened) > 0:
        cash_for_trade_per_share = (float(account.non_marginable_buying_power) - CASH_LIMIT) / len(screened)
        for item in screened:
            stock = item['stock']
            operation = 'buy' if item['direction'] == 'UP' else 'sell'
            stop_loss = item['stop_loss']
            take_profit = item['take_profit']
            share_price = round(min(stop_loss, take_profit), 2)
            shares_to_trade = int(cash_for_trade_per_share / share_price)
            try:
                if abs(stop_loss - take_profit) > share_price * TAKE_PROFIT_DELTA and shares_to_trade > 0:
                    mensaje = {'stock': stock,'operation': operation,'stop_loss': stop_loss,'take_profit': take_profit,'shares_to_trade': shares_to_trade}
                    mensajes.append(mensaje)
                    print(f'\n{stock} {operation} {stop_loss} {take_profit} {shares_to_trade}')
            except:
                pass
    publish_message_to_topic(mensajes)
    return  {
        "statusCode": 200,
        "body": {"message": 'Screener Function: DONE!'}
    }



# Check stock with TA indicators
def check_stock(stock):
    data = {}
    try:
        df = yf.download(stock, period=SCREENER_PERIOD, interval=SCREENER_INTERVAL)
        if len(df) > 0:
            df['RSI'] = ta.rsi(df['Close'], timeperiod=TA_RSI_TIMEPERIOD)
            bbands = ta.bbands(df['Close'], length=TA_BBANDS_LENGTH, std=TA_BBANDS_STD)
            df['L'] = bbands['BBL_20_2.3']
            df['M'] = bbands['BBM_20_2.3']
            df['U'] = bbands['BBU_20_2.3']

            previous2_bar = df[-3:].head(1)
            previous_bar = df[-2:].head(1)
            current_bar = df[-1:]

            if current_bar['RSI'].values[0] > 70 and current_bar['Close'].values[0] > current_bar['U'].values[0]:
                data = {
                    'direction': 'DOWN',
                    'stock': stock,
                    'stop_loss': round(
                        max(previous_bar['High'].values[0], previous2_bar['High'].values[0],
                            previous_bar['U'].values[0]), 2),
                    'take_profit': round(
                        min(previous_bar['Low'].values[0], previous2_bar['Low'].values[0],
                            previous_bar['M'].values[0]), 2)
                }
            elif current_bar['RSI'].values[0] < 30 and current_bar['Close'].values[0] < current_bar['L'].values[0]:
                data = {
                    'direction': 'UP',
                    'stock': stock,
                    'stop_loss': round(
                        min(previous_bar['Low'].values[0], previous2_bar['Low'].values[0],
                            previous_bar['L'].values[0]), 2),
                    'take_profit': round(
                        max(previous_bar['High'].values[0], previous2_bar['High'].values[0],
                            previous_bar['M'].values[0]), 2)
                }
    except:
        pass

    return data


# Screen stocks
def screen_stocks(trader_api):
    assets = trader_api.list_assets(status='active', asset_class='us_equity')
    assets = [x for x in assets if x.shortable == True and x.exchange == 'NASDAQ']
    stocks = [x.symbol for x in assets][:SCREENER_NASDAQ_COUNT]

    screened = []
    for st in stocks:
        _stock = check_stock(st)
        if _stock != {}:
            screened.append(_stock)

    screened = [x for x in screened if abs(x['stop_loss'] - x['take_profit']) > min(x['stop_loss'], x['take_profit']) * TAKE_PROFIT_DELTA]
    return screened

#Publish message
def publish_message_to_topic(mensajes):
    sns_client = boto3.client('sns', region_name='us-east-1')
    # Convertir la lista de diccionarios a una cadena JSON
    message_json = json.dumps(mensajes)
    
    sns_client.publish(
        TopicArn = PUBSUB_TOPIC_ARN,
        Message=message_json
    )



