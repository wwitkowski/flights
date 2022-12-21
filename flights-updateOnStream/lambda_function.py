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
from .table import FlightsTable
from .utils import origin_map, is_new_flight, flight_to_text

logger = logging.getLogger()
logger.setLevel(logging.INFO)
dyn_resource = boto3.resource(
    'dynamodb'
    # aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    # aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    # region_name='us-east-1'
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
    message["To"] = ', '.join(receiver_email)
    html = f"""
    <html> 
        <body> 
            <p>{'<br>'.join([flight_to_text(flight) for flight in flights])}</p>
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
    thresholds = [0.75, 0.65, 0.5]
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
                'flight_info': flight_info, 
                'flight_details': flight_details,
                'median_price': median_price,
                    'median_price': median_price, 
                'median_price': median_price,
                    'median_price': median_price, 
                'median_price': median_price,
            }
        
            for threshold, level in zip(thresholds, levels):
                if new_price < median_price - threshold*median_price:
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
