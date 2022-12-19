from datetime import datetime

origin_map = {
    'KTW': 'Katowice',
    'KRK': 'Kraków',
    'WAW': 'Warszawa'
}


def flight_to_text(flight):
    fire_emojis = '&#128293;'*flight['level']
    origin = flight['flight_info']['origin_place_id']
    destination = flight['flight_info']['destination_place_id']
    destination_name = f"{flight['flight_info']['destination_city']}, {flight['flight_info']['destination_country']}"
    departure_date = datetime.strptime(flight['flight_details']['departure_date'], '%Y%m%d').date().isoformat()
    return_date = datetime.strptime(flight['flight_details']['return_date'], '%Y%m%d').date().isoformat()
    days = flight['flight_details']['days']
    current_price = flight['flight_details']['price']
    median_price = flight['median_price']
    url = f"https://www.kayak.pl/flights/{origin}-{destination}/{departure_date}-flexible-3days/{return_date}-flexible-3days?sort=bestflight_a"
    return f'{fire_emojis} {origin} - {destination} ({destination_name}): '\
        f'Od {departure_date} do {return_date} ({days} dni) za <b>{current_price} zł</b> (Mediana: {median_price:.2f} zł) '\
        f'<a href="{url}">LINK</a>'


def is_new_flight(record):
    if record['eventName'] != 'INSERT':
        return False
    if not 'cid' in record['dynamodb']['NewImage']['SortKey']['S']:
        return False
    return True