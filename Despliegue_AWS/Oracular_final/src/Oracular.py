import json
from yahoo_fin import stock_info as yf
import numpy as np
import datetime as dt
import time as tm
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import LSTM, Dropout, Dense
from collections import deque
import requests as rq
import boto3

# SETTINGS

TELEGRAM_URL = 'https://api.telegram.org'
TELEGRAM_BOT_ID = 'bot6240899521:AAEjX6ayWtTb1FSU-OIZ8lbCoX7szwA3_jk'
TELEGRAM_CHAT_ID = '-1002121727585'

AWS_REGION = 'us-east-1'
ORACULAR_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:232041705264:TradingOracularTopic'
ORACULAR_ID ='Función Oracular'


PUBSUB_TIMEOUT = 5.0

N_STEPS = 7

LOOKUP_STEPS = [1, 2, 3]

def lambda_handler(event, context):
  
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
  mensajes = []
  if len(info_completa) > 0:
    date_now = tm.strftime('%Y-%m-%d')
    date_3_years_back = (dt.date.today() - dt.timedelta(days=1104)).strftime('%Y-%m-%d')

  for stock_item in info_completa:
    init_df = yf.get_data(stock_item['stock'], start_date=date_3_years_back, end_date=date_now, interval='1d')
    init_df = init_df.drop(['open', 'high', 'low', 'adjclose', 'ticker', 'volume'], axis=1)
    init_df['date'] = init_df.index
    scaler = MinMaxScaler()
    init_df['close'] = scaler.fit_transform(np.expand_dims(init_df['close'].values, axis=1))
    #Empezamos a tomar las predicciones
    predictions = []
    for step in LOOKUP_STEPS:
      df, last_sequence, x_train, y_train = PrepareData(step, init_df)
      x_train = x_train[:, :, :len(['close'])].astype(np.float32)
      model = GetTrainedModel(x_train, y_train)
      last_sequence = last_sequence[-N_STEPS:]
      last_sequence = np.expand_dims(last_sequence, axis=0)
      prediction = model.predict(last_sequence)
      predicted_price = scaler.inverse_transform(prediction)[0][0]
      predictions.append(round(float(predicted_price), 2))

    if len(predictions) == len(LOOKUP_STEPS):
        mensaje = {'stock': stock_item['stock'],'day_1': str(predictions[0]),'day_2': str(predictions[1]),'day_3': str(predictions[2],),'operation': stock_item['operation'],'stop_loss': stock_item['stop_loss'],'take_profit': stock_item['take_profit'],'shares_to_trade': stock_item['shares_to_trade']}
        mensajes.append(mensaje)
        predictions_list = [str(d)+'$' for d in predictions]
        predictions_str = ', '.join(predictions_list)
        message_telegram = f"{ORACULAR_ID}: *{stock_item['stock']}* prediction for upcoming 3 days ({predictions_str})"
        send_message(message_telegram)

  publish_message_to_topic(mensajes)
  return {'message': f'{ORACULAR_ID}: execution DONE!'}

# Send message to Telegram channel
def send_message(message):
  response = rq.post(
        f'{TELEGRAM_URL}/{TELEGRAM_BOT_ID}/sendMessage?chat_id={TELEGRAM_CHAT_ID}&parse_mode=Markdown&text={message}')

  return response

#Publish message
def publish_message_to_topic(mensajes):
    sns_client = boto3.client('sns', region_name='us-east-1')
    # Convertir la lista de diccionarios a una cadena JSON
    message_json = json.dumps(mensajes)
    sns_client.publish(
        TopicArn = ORACULAR_SNS_TOPIC_ARN,
        Message=message_json
    )

#Prepare data
def PrepareData(days, init_df):
  df = init_df.copy()
  df['future'] = df['close'].shift(-days)
  last_sequence = np.array(df[['close']].tail(days))
  df.dropna(inplace=True)
  sequence_data = []
  sequences = deque(maxlen=N_STEPS)
  for entry, target in zip(df[['close'] + ['date']].values, df['future'].values):
      sequences.append(entry)
      if len(sequences) == N_STEPS:
          sequence_data.append([np.array(sequences), target])
  last_sequence = list([s[:len(['close'])] for s in sequences]) + list(last_sequence)
  last_sequence = np.array(last_sequence).astype(np.float32)

  # construct the X's and Y's
  X, Y = [], []
  for seq, target in sequence_data:
      X.append(seq)
      Y.append(target)
  # convert to numpy arrays
  X = np.array(X)
  Y = np.array(Y)
  return df, last_sequence, X, Y


def GetTrainedModel(x_train, y_train):
  model = Sequential()
  model.add(LSTM(60, return_sequences=True, input_shape=(N_STEPS, len(['close']))))
  model.add(Dropout(0.3))
  model.add(LSTM(120, return_sequences=False))
  model.add(Dropout(0.3))
  model.add(Dense(20))
  model.add(Dense(1))

  BATCH_SIZE = 8
  EPOCHS = 80

  model.compile(loss='mean_squared_error', optimizer='adam')

  model.fit(x_train, y_train,
            batch_size=BATCH_SIZE,
            epochs=EPOCHS,
            verbose=0)

  model.summary()

  return model
