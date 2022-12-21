from math import sin, cos, sqrt, atan2, radians
origin_coordinates_map = {
    'KTW': {'lon': 19.08002, 'lat': 50.47425, 'limit': 4000},
    'KRK': {'lon': 20.96712, 'lat': 52.16575, 'limit': 4000},
    'WAW': {'lon': 19.79157, 'lat': 50.07617, 'limit': 4000}
}

def distance(origin, destination_lat, destination_lon):
    earth_radius = 6373.0

    lat1 = radians(origin_coordinates_map[origin]['lat'])
    lon1 = radians(origin_coordinates_map[origin]['lon'])
    lat2 = radians(destination_lat)
    lon2 = radians(destination_lon)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return earth_radius * c

def is_long(trip):
    origin = trip['originAirportShortName']
    if distance(
        origin, 
        trip['airport']['latitude'], 
        trip['airport']['longitude']
    ) > origin_coordinates_map[origin]['limit']:
        return True
    return False
