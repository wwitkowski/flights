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

logger = logging.getLogger()
logger.setLevel(logging.INFO)
dyn_resource = boto3.resource(
    'dynamodb'
    # aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    # aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    # region_name='us-east-1'
)
table = dyn_resource.Table('flights')
origin_map = {
    'KTW': 'Katowice',
    'KRK': 'Kraków',
    'WAW': 'Warszawa'
}


def flight_to_text(flight):
    fire_emojis = '&#128293;'*flight['level']
    origin = flight['flight_details']['origin_place_id']
    destination = flight['flight_details']['destination_place_id']
    destination_name = f"{flight['flight_details']['destination_city']}, {flight['flight_details']['destination_country']}"
    departure_date = datetime.strptime(flight['current_flight']['departure_date'], '%Y%m%d').date().isoformat()
    return_date = datetime.strptime(flight['current_flight']['return_date'], '%Y%m%d').date().isoformat()
    days = flight['current_flight']['days']
    current_price = flight['current_flight']['price']
    mean_price = flight['mean_price']
    url = f"https://www.kayak.pl/flights/{origin}-{destination}/{departure_date}-flexible-3days/{return_date}-flexible-3days?sort=bestflight_a"
    return f'{fire_emojis} {origin} - {destination} ({destination_name}): '\
        f'Od {departure_date} do {return_date} ({days} dni) za <b>{current_price} zł</b> (Średnia: {mean_price:.2f} zł) '\
        f'<a href="{url}">LINK</a>'


def send_email(flights):
    sender_email = os.environ.get('GMAIL_FROM')
    receiver_email = os.environ.get('GMAIL_TO').split(',')
    title_origin = origin_map[flights[0]['flight_details']['origin_place_id']]
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
    if table.exists():
        records = event['Records']
        for record in records:
            if record['eventName'] != 'INSERT':
                continue
            if record['dynamodb']['NewImage']['SortKey']['S'] in ['total_agg', 'details']:
                continue
            flight_id = record['dynamodb']['Keys']['FlightID']['S']
            new_price = record['dynamodb']['NewImage']['price']['N']
            current_flight = {
                'price': new_price,
                'days': record['dynamodb']['NewImage']['days']['N'],
                'departure_date': record['dynamodb']['NewImage']['departure_date']['S'],
                'return_date': record['dynamodb']['NewImage']['return_date']['S']
            }
            response = table.query_items('FlightID', flight_id)
            flight_info = [item for item in response if 'details' in item['SortKey']][0]
            prices = [float(item['price']) for item in response if 'cid' in item['SortKey']]
            if len(prices) < 5:
                continue
            mean_price = np.mean(prices)
            q3, q1 = np.percentile(prices, [75, 25])
            iqr = q3 - q1
            thrshold_lvl1 = q1 - 0.5 * iqr
            thrshold_lvl2 = q1 - 1 * iqr
            thrshold_lvl3 = q1 - 1.5 * iqr
            cheap_flight = {
                    'flight_details': flight_info, 
                    'mean_price': mean_price, 
                    'current_flight': current_flight, 
                }
            new_price = float(new_price)
            if new_price < thrshold_lvl3:
                cheap_flights.append({
                    'level': 3,
                    **cheap_flight
                })
            elif new_price < thrshold_lvl2:
                cheap_flights.append({ 
                    'level': 2,
                    **cheap_flight
                })
            elif new_price < thrshold_lvl1:
                cheap_flights.append({
                    'level': 1,
                    **cheap_flight
                })
            i += 1
            time.sleep(0.15)
    logger.info('Checked %d items', i)
    if cheap_flights:
        logger.info('Sending mail')
        send_email(cheap_flights)
    logger.info('Done')
    return {
        'statusCode': 200,
    } 
