import json
import boto3
import logging
from decimal import Decimal
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)
dyn_resource = boto3.resource('dynamodb')
table = dyn_resource.Table('flights')

def lambda_handler(event, context):
    logger.info('Start')
    logger.info('%s' % event)
    
    records = event['Records']
    record = records[0]
    if record['dynamodb']['NewImage']['SortKey']['S'] in ['total_agg', 'details']:
        return {
            'statusCode': 201,
            'body': 'Not a price item. Accepted.'
        }
    try:
        flight_id = record['dynamodb']['Keys']['FlightID']['S']
        new_price = record['dynamodb']['NewImage']['price']['N']
        logger.info('%s' % new_price)
        response = table.update_item(
                Key={'FlightID': flight_id, 'SortKey': 'total_agg'},
                UpdateExpression="set flights_month_total_price = flights_month_total_price + :val, flights_month_count = flights_month_count + :inc",
                ExpressionAttributeValues={':val': Decimal(str(new_price)), ':inc': Decimal(str(1))},
                ReturnValues="UPDATED_NEW")
    except ClientError as err:
        logger.error(
                "Couldn't update. Here's why: %s: %s",
                err.response['Error']['Code'],
                err.response['Error']['Message']
            )
        raise
    else:
        return {
            'statusCode': 200,
            'body': response['Attributes']
        } 