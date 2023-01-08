import os
import smtplib
import ssl
import boto3
import time
import logging
import numpy as np
from datetime import datetime 
from email.mime.text import MIMEText 
from email.mime.multipart import MIMEMultipart 
from table import FlightsTable
from utils import origin_map, is_new_flight, flight_to_text

logger = logging.getLogger()
logger.setLevel(logging.INFO)
dyn_resource = boto3.resource(
    'dynamodb',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name='us-east-1'
)


def send_email(flights):
    sender_email = os.environ.get('GMAIL_FROM')
    receiver_email = os.environ.get('GMAIL_TO').split(',')
    title_origin = origin_map[flights[0]['flight_info']['origin_place_id']]
    date = datetime.today().date().isoformat()
    password = os.environ.get('GMAIL_PASSWORD')
    
    message = MIMEMultipart("alternative") 
    message["Subject"] = f"Tanie loty {title_origin} - {date}" 
    message["From"] = sender_email
    short_Flights = '<br>'.join([flight_to_text(flight) for flight in flights if 'BREAK' in flight['flight_id']])
    week_flights = '<br>'.join([flight_to_text(flight) for flight in flights if 'WEEK' in flight['flight_id']])
    long_flights = '<br>'.join([flight_to_text(flight) for flight in flights if 'LONG' in flight['flight_id']])
    short_flights_text = f"KRÓTKIE (3 - 5 dni):<br>{short_Flights}<br><br>"
    week_flights_text = f"TYGODNIOWE (6 - 9 dni):<br>{week_flights}<br><br>"
    long_flights_text = f"DŁUGIE (10 - 13 dni):<br>{long_flights}"
    flights_text = ''
    if short_Flights:
        flights_text += short_flights_text
    if week_flights:
        flights_text += week_flights_text
    if long_flights:
        flights_text += long_flights_text
    if not flights_text:
        return
    html = f"""
    <html> 
        <body>
            <p>{flights_text}</p>
        </body> 
    </html> 
    """ 
    message_body = MIMEText(html, "html") 
    message.attach(message_body) 
    context = ssl.create_default_context() 
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server: 
        server.login(sender_email, password) 
        server.sendmail( 
            sender_email, receiver_email, message.as_string()
        )


def lambda_handler(event, context):
    i = 0
    table = FlightsTable(dyn_resource)
    cheap_flights = []
    price_number_limit = 15
    percentiles = [32, 5, 1]
    levels = [3, 2, 1]
    if table.exists():
        records = event['Records']
        for record in records:
            if not is_new_flight(record):
                continue
            i += 1
            flight_id = record['dynamodb']['Keys']['FlightID']['S']
            response = table.query_items('FlightID', flight_id)
            flight_info = [item for item in response if 'details' in item['SortKey']][0]
            prices = [float(item['price']) for item in response if 'cid' in item['SortKey']]
            if len(prices) < price_number_limit:
                continue

            new_price = float(record['dynamodb']['NewImage']['price']['N'])
            median_price = np.median(prices)
            flight_details = {
                'price': new_price,
                'days': record['dynamodb']['NewImage']['days']['N'],
                'departure_date': record['dynamodb']['NewImage']['departure_date']['S'],
                'return_date': record['dynamodb']['NewImage']['return_date']['S']
            }
            cheap_flight = {
                'flight_id': flight_id,
                'flight_info': flight_info, 
                'flight_details': flight_details,
                'median_price': median_price
            }
            thresholds = list(np.percentile(prices, percentiles))
            for threshold, level in zip(thresholds, levels):
                if new_price < threshold:
                    cheap_flights.append({
                        'level': level,
                        **cheap_flight
                    })
                    break
            time.sleep(0.15)

    logger.info('Checked %d items', i)
    if cheap_flights:
        logger.info('Sending mail')
        send_email(cheap_flights)
    
    logger.info('Done')
    return {
        'statusCode': 200,
    } 

event = {
  "Records": [
    {
      "eventID": "af61846c00533088012f24e65552d1cd",
      "eventName": "INSERT",
      "eventVersion": "1.1",
      "eventSource": "aws:dynamodb",
      "awsRegion": "us-east-1",
      "dynamodb": {
        "ApproximateCreationDateTime": 1671286078,
        "Keys": {
          "FlightID": {
            "S": "KTW-STR-BREAK"
          },
          "SortKey": {
            "S": "cid_20221217"
          }
        },
        "NewImage": {
          "collection_date": {
            "S": "2022-12-17"
          },
          "price": {
            "N": "640"
          },
          "FlightID": {
            "S": "KTW-STR-BREAK"
          },
          "departure_date": {
            "S": "20230103"
          },
          "days": {
            "N": "3"
          },
          "SortKey": {
            "S": "cid_20221217"
          },
          "return_date": {
            "S": "20230106"
          }
        },
        "SequenceNumber": "92098800000000044656506229",
        "SizeBytes": 161,
        "StreamViewType": "NEW_AND_OLD_IMAGES"
      },
      "eventSourceARN": "arn:aws:dynamodb:us-east-1:111884791752:table/flights/stream/2022-11-29T17:34:28.591"
    }
  ]
}

lambda_handler(event, None)