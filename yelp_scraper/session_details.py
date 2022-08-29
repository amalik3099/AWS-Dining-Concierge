import requests
import json
import argparse
import sys
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.parse import urlencode

CLIENT_ID = 'ac4CEGgxxwnUi9ydysM2yQ'
API_KEY = 'M_MlUi2bI7xy9ykrLIhNiKOPck0pvb73uN5jkWv8LNPVPuOBEur_TAOM8UqZv1Ew_0MZBsMR_FgBr-yNIxNgCMP5FN4QnIksukfNIjlQLwQpu0EAVC_2ZhCGjkoIYnYx'
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'
SEARCH_LIMIT = 50


def search(api_key, term, location):
    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT
    }
    url = '{0}{1}'.format(API_HOST, quote(SEARCH_PATH.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    res = requests.request('GET', url, headers=headers, params=url_params)
    return res.json()


def search_yelp(term, location):
    res = search(API_KEY, term, location)
    matches = res.get('businesses')

    if not matches:
        print(u'No businesses for {0} in {1} found.'.format(term, location))
        return

    print(u'\t{0} biz found'.format(len(matches)))
    return matches


def get_locations():
    locations = ["Sutton Place, NY", "Rockefeller Center, NY", "Diamond District, NY", "Theater District, NY",
                 "Washington Heights, NY", "Hudson Heights, NY", "West Harlem, NY", "Hamilton Heights, NY", "Manhattanville, NY", "Morningside Heights, NY", "Central Harlem, NY",
                 "Turtle Bay, NY", "Midtown East, NY", "Midtown, NY", "Tudor City, NY", "Little Brazil, NY", "Times Square, NY", "Hudson Yards, NY", "Bronx, NY", "Flushing, NY",
                 "Midtown West, NY", "Hell's Kitchen, NY", "Garment District, NY", "Herald Square, NY", "Koreatown, NY", "Murray Hill, NY", "Tenderloin, NY", "Bayside, NY",
                 "Madison Square, NY", "Flower District, NY", "Brookdale, NY", "Hudson Yards, NY", "Kips Bay, NY", "Rose Hill, NY", "NoMad, NY", "Peter Cooper Village, NY",
                 "Chelsea, NY", "Flatiron District, NY", "Gramercy Park, NY", "Stuyvesant Square, NY", "Union Square, NY", "Stuyvesant Town, NY", "Meatpacking District, NY",
                 "Harlem, NY", "St. Nicholas Historic District, NY", "Astor Row, NY", "Marcus Garvey Park, NY", "Le Petit Senegal, NY", "East Harlem, NY",
                 "Waterside Plaza, NY", "Downtown Manhattan, NY", "Little Germany, NY", "Alphabet City, NY", "East Village, NY", "Greenwich Village, NY", "NoHo, NY",
                 "Bowery, NY", "West Village, NY", "Lower East Side	, NY", "SoHo, NY", "Nolita, NY", "Little Australia, NY", "Little Italy, NY", "Chinatown	, NY",
                 "Financial District, NY", "Five Points, NY", "Cooperative Village, NY", "Two Bridges, NY", "Tribeca, NY", "Civic Center, NY", "Radio Row , NY",
                 "South Street Seaport, NY", "Battery Park City, NY", "Little Syria, NY", "Upper Manhattan, NY", "Marble Hill, NY", "Inwood, NY", "Fort Georg, NY",
                 "Brooklyn Heights, NY", "Dumbo, NY", "Williamsburg, NY", "Greenpoint, NY", "Cobble Hill, NY", "Sunnyside, NY", "Astoria, NY", "Woodside, NY",
                 "Upper East Side, NY", "Lenox Hill, NY", "Carnegie Hill, NY", "Yorkville, NY", "Upper West Side, NY", "Manhattan Valley, NY", "Lincoln Square, NY"]
    return locations


def get_cuisines():
    cuisines = ['Chinese restaurants', 'Mexican restaurants', 'Italian restaurants',
                'Thai restaurants', 'Indian restaurants', 'Japanese restaurants', 'Korean restaurants']
    return cuisines
