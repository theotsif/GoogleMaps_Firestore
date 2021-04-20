import pandas as pd
import numpy as np
import math
from GooglePlacesAPI import google_map_API_connection, PlaceSearchMatrix

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

def categories_to_pd_series(category):
    series1_exists = False
    series2_exists = False

    if category[1] is not None:
        series1 = pd.Series([(category[0], 'type_search', i) for i in category[1]])
        series1_exists = True

    if category[2] is not None:
        series2 = pd.Series([(category[0], 'keyword_search', i) for i in category[2]])
        series2_exists = True

    if series1_exists:
        if series2_exists:
            series = pd.concat([series1, series2])
            return series
        else:
            return series1
    else:
        return series2

def radius_approx(area):
    return np.sqrt(area/math.pi)

def places_nearby_details_helper(API_KEY, country, city, row, radius):

    #Connect to the Google Places API
    gmaps = google_map_API_connection(API_KEY=API_KEY)
    places = PlaceSearchMatrix(gmaps)

    place = city + ' ' + country

    #Get coordinates for desired place
    place_id, location_coords = places.choose_place(place=place, start=False)

    if row[1] == 'type_search':
        upd_row = places.google_places_details(
            API_KEY, places.choose_places_nearby(
                coordinates=location_coords, radius=radius, sel_type=row[2])
        )
    else:
        upd_row = places.google_places_details(
            API_KEY, places.choose_places_nearby(
                coordinates=location_coords, keyword_search=True, radius=radius, keyword=row[2])
        )

    if upd_row is not None:

        upd_row.__dict__['_attrs'] = {'category': row[0], 'subcategory': row[2]}

        return upd_row

    else:
        return [None, row[0], row[2]]

def write_subcategory_countries_FireCloud_helper(Firebase_Admin_SDK, df, country, city):

    # Write to FireCloud
    cred = credentials.Certificate(Firebase_Admin_SDK)

    try:
        firebase_admin.initialize_app(cred)
    except:
        pass

    db = firestore.client()
    batch = db.batch()

    # Write array of dicts as a collection to FireCloud
    if type(df) != list:
        category = df.__dict__['_attrs']['category']
        subcategory = df.__dict__['_attrs']['subcategory']
        df_arr = df.to_dict(orient='records')
        for doc in df_arr:
            doc['name'] = doc['name'].replace('/', ' ')
            print(doc['name'])
            docRef = db.collection('Countries').document(country).collection(city). \
                document(category).collection(subcategory).document(doc['name'])
            batch.set(docRef, doc, merge=True)
    else:
        category = df[1]
        subcategory = df[2]
        doc = {'Results':None}
        docRef = db.collection('Countries').document(country).collection(city). \
            document(category).collection(subcategory).document('Results')
        batch.set(docRef, doc, merge=True)

    # Commit the batch
    batch.commit()

def write_FireCloud(API_KEY, Firebase_Admin_SDK, df, radius, country, city):
    return df.apply(lambda x: write_subcategory_countries_FireCloud_helper(
        Firebase_Admin_SDK, places_nearby_details_helper(API_KEY, country, city, x, radius), country, city))

def GoogleMapsAPICategories_toFireCloud(API_KEY, Firebase_Admin_SDK, categories, country, city, area_km2):
    categories_series = pd.concat(list(map(categories_to_pd_series, categories)))
    area = area_km2 * 10 ** 6
    radius = radius_approx(area)

    return write_FireCloud(API_KEY, Firebase_Admin_SDK, categories_series, radius, country, city)