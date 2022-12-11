import os
import boto3
import time
import logging
import statistics
from .table import FlightsTable

logger = logging.getLogger()
logger.setLevel(logging.INFO)
dyn_resource = boto3.resource(
    'dynamodb',
    # aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    # aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    # region_name='us-east-1'
)
table = dyn_resource.Table('flights')

def lambda_handler(event, context):

    i = 0
    table = FlightsTable(dyn_resource)
    if table.exists():
        records = event['Records']
        for record in records:
            if record['eventName'] != 'INSERT':
                msg = 'Not an insert event.'
                logger.info(msg)
                continue
            if record['dynamodb']['NewImage']['SortKey']['S'] in ['total_agg', 'details']:
                msg = 'Not a price item.'
                logger.info(msg)
                continue
            flight_id = record['dynamodb']['Keys']['FlightID']['S']
            new_price = float(record['dynamodb']['NewImage']['price']['N'])
            response = table.query_items('FlightID', flight_id)
            prices = [item['price'] for item in response if 'cid' in item['SortKey']]
            mean_price = statistics.mean(prices)
            thrshold = mean_price - 2*statistics.stdev(prices)
            if new_price < thrshold:
                logger.info(
                    '!!!! Found cheap flight! Flight: %s, Mean price: %s, Current price: %s', 
                    flight_id, mean_price, new_price
                    )
            i += 1
            time.sleep(0.15)
    logger.info('Checked %d items', i)
    return {
        'statusCode': 200,
    } 
