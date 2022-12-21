import os
import boto3
import logging
import requests
import datetime as dt
from collections import defaultdict, namedtuple
from .table import FlightsTable
from .utils import is_long


HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'pl,en-US;q=0.7,en;q=0.3',
    'Connection': 'keep-alive',
    'Host': 'www.kayak.pl',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'TE': 'trailers',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0',
}
URL_ENDPOINT = 'https://www.kayak.pl/s/horizon/exploreapi/destinations?'\
    'airport={d[origin_place_id]}&'\
    'budget={d[budget]}&'\
    'tripdurationrange={d[days_range]}&'\
    'duration=&'\
    'flightMaxStops={d[max_stops]}&'\
    'stopsFilterActive=false&topRightLat=80&topRightLon=180&bottomLeftLat=-65&bottomLeftLon=-180&zoomLevel=1&'\
    'selectedMarker=&themeCode={d[theme]}&selectedDestination='
TODAY = dt.datetime.today()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
Flight = namedtuple('Flight', ['route_details', 'flight_details'])
dyn_resource = boto3.resource(
    'dynamodb'
)


def get_trips(origin, **kwargs):
    url = URL_ENDPOINT.format(d=defaultdict(str, origin_place_id=origin, **kwargs))
    r = requests.get(url=url, headers=HEADERS)
    r_json = r.json()
    return r_json


def parse_trip(trip, trip_type):
    key = f"{trip['originAirportShortName']}-{trip['airport']['shortName']}-{trip_type}"
    if trip['days'] == 0 or trip['flightInfo']['price'] > 20000:
        return 
    if trip_type in ['BREAK', 'WEEK'] and is_long(trip):
        return
    route_details = {
        'FlightID': key,
        'SortKey': 'details',
        'origin_place_id': trip['originAirportShortName'],
        'destination_place_id': trip['airport']['shortName'],
        'destination_city': trip['city']['name'],
        'destination_country': trip['country']['name']
    }
    flight_details = {
        'FlightID': key,
        'SortKey': f'cid_{TODAY.strftime("%Y%m%d")}',
        'collection_date': TODAY.strftime('%Y-%m-%d'),
        'departure_date': trip['departd'],
        'return_date': trip['returnd'],
        'price': trip['flightInfo']['price'],
        'days': trip['days']
    }
    return Flight(route_details, flight_details)


def lambda_handler(event, context):
    origin = event['origin']
    days_ranges = {'BREAK': '2,4', 'WEEK': '5,8', 'LONG': '10,15'}
    table = FlightsTable(dyn_resource)
    if table.exists():
        trips_parsed = []
        for trip_type, days_range in days_ranges.items():
            trips = get_trips(origin=origin, days_range=days_range)
            for trip in trips['destinations']:
                parsed_trip = parse_trip(trip, trip_type) 
                if parsed_trip is not None:
                    trips_parsed.append(parsed_trip)
            r = table.write_batch(trips_parsed)
    return {
        'statusCode': 200,
        'body': r
    }
