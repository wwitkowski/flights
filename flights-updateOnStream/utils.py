from datetime import datetime, timedelta

origin_map = {
    'KTW': 'Katowice',
    'KRK': 'Kraków',
    'WAW': 'Warszawa'
}

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days + 1)):
        yield start_date + timedelta(n)

def is_weekend(start_date, end_date):
    dates = [date.weekday() for date in daterange(start_date, end_date)]
    if 5 in dates and 6 in dates:
        return True
    return False

def flight_to_text(flight):
    fire_emojis = '&#128293;'*flight['level']
    origin = flight['flight_info']['origin_place_id']
    destination = flight['flight_info']['destination_place_id']
    destination_name = f"{flight['flight_info']['destination_city']}, {flight['flight_info']['destination_country']}"
    departure_date = datetime.strptime(flight['flight_details']['departure_date'], '%Y%m%d').date()
    return_date = datetime.strptime(flight['flight_details']['return_date'], '%Y%m%d').date()
    days = flight['flight_details']['days']
    weekend_emoji = '&#128718;' if int(days) < 5 and is_weekend(departure_date, return_date) else ''
    current_price = flight['flight_details']['price']
    median_price = flight['median_price']
    url = f"https://www.kayak.pl/flights/{origin}-{destination}/"\
          f"{departure_date.isoformat()}-flexible-3days/"\
          f"{return_date.isoformat()}-flexible-3days??fs=cfc=1&sort=bestflight_a"
    return f'{weekend_emoji}{fire_emojis} {origin} - {destination} ({destination_name}): '\
        f'Od {departure_date.isoformat()} do {return_date.isoformat()} ({days} dni) za <b>{current_price} zł</b> (Mediana: {median_price:.2f} zł) '\
        f'<a href="{url}">LINK</a>'


def is_new_flight(record):
    if record['eventName'] != 'INSERT':
        return False
    if not 'cid' in record['dynamodb']['NewImage']['SortKey']['S']:
        return False
    return True