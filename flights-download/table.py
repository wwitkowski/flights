import time
import json
import logging
from decimal import Decimal
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class DynamoDBTable:
    """Encapsulates an Amazon DynamoDB table."""
    table_name = None

    
    def __init__(self, dyn_resource):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.dyn_resource = dyn_resource
        self.table = None

    def exists(self):
        """
        Determines whether a table exists. As a side effect, stores the table in
        a member variable.
        :param table_name: The name of the table to check.
        :return: True when the table exists; otherwise, False.
        """
        try:
            table = self.dyn_resource.Table(self.table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                logger.error(
                    "Unable to check if table %s exists. %s: %s", self.table_name, 
                    err.response['Error']['Code'], err.response['Error']['Message']
                )
                raise
        else:
            self.table = table
        return exists

    def write_batch(self, items: list):
        """
        Fills an Amazon DynamoDB table with the specified data, using the Boto3
        Table.batch_writer() function to put the items in the table.
        Inside the context manager, Table.batch_writer builds a list of
        requests. On exiting the context manager, Table.batch_writer starts sending
        batches of write requests to Amazon DynamoDB and automatically
        handles chunking, buffering, and retrying.
        :param items: The data to put in the table. Each item must contain at least
                       the keys required by the schema that was specified when the
                       table was created.
        """
        try:
            with self.table.batch_writer() as writer:
                for item in items:
                    writer.put_item(Item=json.loads(json.dumps(item), parse_float=Decimal))
        except ClientError as err:
            logger.error(
                "Couldn't load data into table %s. %s: %s", self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return json.dumps(f'Successfully uploaded {len(items)} items')

    def add_item(self, item: dict):
        """
        Adds a item to the table.
        :param item: The item to be added.
        """
        try:
            parsed_item = json.loads(json.dumps(item), parse_float=Decimal)
            self.table.put_item(Item=parsed_item)
        except ClientError as err:
            logger.error(
                "Couldn't add item to the table. %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return json.dumps(item)

    def get_item(self, key: dict):
        """
        Gets single item from the table.
        :param key: Key of the item in the database.
        :return: Item data.
        """
        try:
            response = self.table.get_item(Key=key)
        except ClientError as err:
            logger.error(
                "Couldn't get item %s. %s: %s", key,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response

    def query_items(self, key, value, fields: list = None):
        """
        Queries for items that match specifier key=value criteria.
        Optionally return only specified fields.
        :param key: Item key.
        :param value: Key value.
        :param fields: columns to be returned.
        :return: Matched items.
        """
        p_expression = ','.join(fields)
        try:
            response = self.table.query(KeyConditionExpression=Key(key).eq(value), ProjectionExpression=p_expression)
            data = response['Items']
            while 'LastEvaluatedKey' in response:
                response = self.table.query(
                    KeyConditionExpression=Key(key).eq(value), 
                    ProjectionExpression=p_expression, 
                    ExclusiveStartKey=response['LastEvaluatedKey']
                ) 
                data.extend(response['Items'])
                time.sleep(0.25)
        except ClientError as err:
            logger.error(
                "Query failed: %s: %s", err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return data


class FlightsTable(DynamoDBTable):
    table_name = 'flights'

    def __init__(self, dyn_resource, backoff=1, retries=5):
        super().__init__(dyn_resource)
        self.backoff = backoff
        self.retries = retries

    
    def write_item(self, writer, item):
        i = 0
        while True:
            try:
                writer.put_item(Item=json.loads(json.dumps(item), parse_float=Decimal))
                return
            except ClientError as err:
                if err.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                    if i == self.retries:
                        logger.error(
                            "Couldn't load data into table after %d retries %s. %s: %s", i, self.table.name,
                            err.response['Error']['Code'], err.response['Error']['Message'])
                        raise
                    sleep = (self.backoff * 2 ** i)
                    logger.warning("Throughput Exceeded. Sleeping for ", sleep)
                    time.sleep(sleep)
                    i += 1
                else:
                    logger.error(
                        "Couldn't load data into table %s. %s: %s", self.table.name,
                        err.response['Error']['Code'], err.response['Error']['Message'])
                    raise


    def write_batch(self, items: list):
        with self.table.batch_writer() as writer:
            for item in items:
                key = item[0]['FlightID']
                details_key = self.get_item({'FlightID': key, 'SortKey': 'details'})
                logger.info('Writing item: %s', key)
                if not details_key.get('Item'):
                    self.write_item(writer, item.route_details)
                    agg_key = {'FlightID': key, 'SortKey': 'total_agg', 'count_total': 0, 'price_total': 0}
                    self.write_item(writer, agg_key)
                self.write_item(writer, item.flight_details)
        logger.info('Successfully uploaded %d items', len(items))
        return json.dumps(f'Successfully uploaded {len(items)} items')

    def __repr__(self):
        return '%s.DynamoDBTable object' % self.table_name
