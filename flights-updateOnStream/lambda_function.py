import json
import time
import boto3
import logging
from decimal import Decimal
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)
dyn_resource = boto3.resource('dynamodb')
table = dyn_resource.Table('flights')

def lambda_handler(event, context):

    i = 0
    records = event['Records']
    for record in records:
        if record['eventName'] != 'INSERT':
            msg = 'Not an insert event. Accepted.'
            logger.info(msg)
        if record['dynamodb']['NewImage']['SortKey']['S'] in ['total_agg', 'details']:
            msg = 'Not a price item. Accepted.'
            logger.info(msg)
        try:
            flight_id = record['dynamodb']['Keys']['FlightID']['S']
            new_price = record['dynamodb']['NewImage']['price']['N']
            response = table.update_item(
                Key={'FlightID': flight_id, 'SortKey': 'total_agg'},
                UpdateExpression="set price_total = price_total + :val, count_total = count_total + :inc",
                ExpressionAttributeValues={':val': Decimal(str(new_price)), ':inc': Decimal(str(1))},
                ReturnValues="UPDATED_NEW"
            )
            logger.info('Updated: %s', flight_id)
            i += 1
            time.sleep(0.15)
        except ClientError as err:
            logger.error(
                    "Couldn't update. Here's why: %s: %s",
                    err.response['Error']['Code'],
                    err.response['Error']['Message']
                )
            raise
    return {
        'statusCode': 200,
        'body': json.dumps('Update')
    } 