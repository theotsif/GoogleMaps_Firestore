import googlemaps
import pandas as pd
import requests
import time

    
#Connect to the API
def google_map_API_connection(API_KEY=None):
    gmaps = googlemaps.Client(key = API_KEY)
    return gmaps 


class PlaceSearchMatrix:
    
        def __init__(self, gmaps): 
            self.gmaps = gmaps
         
        def choose_place(self, place, start=False):
            
            gmaps = self.gmaps
            
            #Choose city
            city_result  = gmaps.places_autocomplete(place)
            first_place_id = city_result[0]['place_id']
            sel_places_field  = gmaps.place(first_place_id)
            lat_lon_coord = sel_places_field['result']['geometry']['location']
            location_coords = (str(lat_lon_coord['lat']), str(lat_lon_coord['lng']))
            if start:
                return first_place_id, location_coords, sel_places_field
            else:
                return first_place_id, location_coords

        def choose_places_nearby(self, coordinates, radius, keyword_search=False, sel_type=None, keyword=None):

            '''
           Select places withing the radius specified. By default, each Nearby Search returns up to 20 establishment
           results per query only. However, each search can return as many as 60 results, split across three pages.
            '''
            
            gmaps = self.gmaps
            
            #Search by keyword:
            if keyword_search:
                places_nearby  = gmaps.places_nearby(location=coordinates,
                                       radius = radius,  keyword=keyword)
            
            #Search by type
            else:
                places_nearby  = gmaps.places_nearby(location=coordinates,
                                           radius = radius,  type = sel_type)

            next_page_token = False
            if 'next_page_token' in places_nearby:
                time.sleep(2)
                places_nearby_2 = gmaps.places_nearby(page_token=places_nearby['next_page_token'])
                places_nearby['results'] = places_nearby['results'] + places_nearby_2['results']

                if 'next_page_token' in places_nearby_2:
                    time.sleep(2)
                    places_nearby_3 = gmaps.places_nearby(page_token=places_nearby_2['next_page_token'])
                    places_nearby['results'] = places_nearby['results'] + places_nearby_3['results']

            mykeys = ['place_id', 'name', 'vicinity', 'types', 'user_ratings_total', 'rating']
            places_fields = []
            if len(places_nearby['results']) != 0:
                for place in places_nearby['results']:

                    filtered_dict = {k:v for (k,v) in place.items() if k in mykeys}
                    filtered_dict['location'] = (place['geometry']['location']['lat'],
                                                 place['geometry']['location']['lng'])
                    places_fields.append(filtered_dict)

                places_fields = pd.DataFrame(places_fields)

                if keyword_search:
                    if 'types' in places_fields.columns:
                        places_fields['types'] = places_fields.types.apply(lambda x:x+[keyword])
                    else:
                        places_fields['types'] = [keyword]

                #print(places_fields.columns)

                places_fields['formatted_address'] = places_fields['place_id'].apply(
                                                         lambda x: gmaps.reverse_geocode(x)[0]['formatted_address'])

                places_fields = places_fields.reset_index(drop=True)
                return pd.DataFrame(places_fields)

            else:
                return None
        
        def google_places_details(self, API_KEY, df):
    
            '''
            Query the place details api and extract opening hours and price levels (where they exist) 
            '''
            url = 'https://maps.googleapis.com/maps/api/place/details/json'

            places_add_fields = []

            if df is not None:

                for i in df['place_id']:

                    params = dict(key=API_KEY, place_id=i)
                    res = requests.get(url, params=params)
                    json = res.json()

                    results = [json['result'].get(key) for key in ['opening_hours','price_level']]

                    if results[0] is None:
                        periods_open = None
                    else:
                        periods_open = results[0]['periods']
                    price_level = results[1]

                    places_add_fields.append([periods_open, price_level])

                return pd.concat([df, pd.DataFrame(places_add_fields,columns=['opening_hours', 'price_level'])], axis=1)

            else:
                return None

