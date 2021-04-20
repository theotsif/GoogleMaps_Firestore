import pandas as pd
import requests

from FireBaseCloudStorage_Manipulation import Read_FireCloud_Collection_Documents, Write_FireCloud_Documents

from GooglePlacesAPI import google_map_API_connection, PlaceSearchMatrix

def Google_API_replace_with_most_popular_helper(API_KEY, country, city, row):
    # Connect to the Google Places API
    gmaps = google_map_API_connection(API_KEY=API_KEY)
    places = PlaceSearchMatrix(gmaps)
    place = '{} {}'.format(city, country)

    # 1. Define empty result
    empty_res = row['name']

    # 2. Pass empty result from places_autocomplete API
    city_result_find_place = gmaps.places_autocomplete('{} {}'.format(empty_res, place))

    # 3. Pass all results from places_autocomplete through places details API
    city_result_find_place_ids = [i['place_id'] for i in city_result_find_place]

    results = []
    for i in city_result_find_place_ids:
        results.append(gmaps.place(place_id=i, fields=['place_id', 'name', 'rating',
                                                       'user_ratings_total', 'price_level']))

    # 4. Pick place with the highest rating
    results_with_ratings = [i['result'] for i in results if "user_ratings_total" in i['result']]

    if results_with_ratings:

        highest_rated_place = sorted(results_with_ratings, key=lambda i: i['user_ratings_total'],
                                     reverse=True)[0]

        # 5. Pass that place from the details API
        sel_places_field = gmaps.place(highest_rated_place['place_id'])

        # Define keys present in Firebase
        sel_keys = ['formatted_address', 'geometry', 'name', 'opening_hours', 'place_id', 'price_level',
                    'rating', 'types', 'user_ratings_total', 'vicinity']

        # Separate fields present and not present in the dictionary
        fields_present = [i for i in sel_keys if i in sel_places_field['result'].keys()]

        fields_not_present = list(set(sel_keys).difference(
            set(fields_present)))
        fields_not_present_nulls = [{i: None} for i in fields_not_present]

        # Pick up the fields needed from the results
        desired_dict = {i: sel_places_field['result'][i] for i in fields_present}

        # Replace geometry with location
        desired_dict['location'] = list(desired_dict['geometry']['location'].values())
        desired_dict.pop('geometry', None)

        # Replace the type fields with the old type as it may include the keyword search field
        desired_dict['types'] = row['types']

        # Add the fields with nulls in the dictionary
        [desired_dict.update(i) for i in fields_not_present_nulls]
    else:
        desired_dict = row.to_dict()

    # Reorder dict as it is written in Firebase
    key_order = ['formatted_address', 'location', 'name', 'opening_hours', 'place_id',
                 'price_level', 'rating', 'types', 'user_ratings_total', 'vicinity']
    desired_dict = {k: desired_dict[k] for k in key_order}

    # Put an initial_Sygic_pass_empty flag
    desired_dict['initial_Sygic_pass_empty'] = True

    return desired_dict

class SygicPass:

    def __init__(self, Firebase_Admin_SDK, Firestore_cllction_path, API_KEY, Sygic_API_KEY, country, city):
        self.Firebase_Admin_SDK = Firebase_Admin_SDK
        self.Firestore_cllction_path = Firestore_cllction_path
        self.collection = Read_FireCloud_Collection_Documents(self.Firebase_Admin_SDK, self.Firestore_cllction_path)
        self.API_KEY = API_KEY
        self.Sygic_API_KEY = Sygic_API_KEY
        self.country = country
        self.city = city

    def pass_collection_through_Sygic_places(self, new_collection=None):

       #check if the method is reused (see method Google_API_replace_with_most_popular below)
       if new_collection:
           collection = new_collection

        # first check if Sygic pass has been done, then include only those if any that miss the Sygic fields
       else:
           collection = [i for i in self.collection if 'Sygic' not in i]

       place = '{} {}'.format(self.city, self.country)
       json_results = []
       for i in [i['name'] for i in collection]:
           search_place = '{} {}'.format(i, place)
           print(search_place)
           url = 'https://api.sygictravelapi.com/1.2/en/places/list?query='

           try:
               res = requests.get(url + search_place, headers={'x-api-key': self.Sygic_API_KEY})
               json = res.json()
               json_results.append(json)

           except:
               json_results.append(None)

       self.json_results = json_results

       return self.json_results

    def eval_Sygic_results(self):
        "evaluate the pass from Sygic and split collections to empty and non-empty"
        Sygic_return_indcs = [i for i, j in enumerate(self.json_results) if j['data']['places']]
        Sygic_empty_indcs = [i for i, j in enumerate(self.json_results) if not j['data']['places']]

        self.collection_empty_from_Sygic = pd.DataFrame(self.collection).iloc[Sygic_empty_indcs]
        self.collection_return_from_Sygic = pd.DataFrame(self.collection).iloc[Sygic_return_indcs]

        return self.collection_empty_from_Sygic, self.collection_return_from_Sygic

    def Google_API_replace_with_most_popular(self):

        #Replace places with those that have more ratings
        collection_empty_Google_updated = self.collection_empty_from_Sygic.apply(
            lambda x: Google_API_replace_with_most_popular_helper(
                API_KEY=self.API_KEY, country=self.country, city=self.city, row=x), axis=1)
        collection_empty_Google_updated = list(collection_empty_Google_updated)
        self.collection_empty_Google_updated = collection_empty_Google_updated

        return self.collection_empty_Google_updated

    def Update_FireBase(self):

        #Delete from FireBase old Places
        Write_FireCloud_Documents(self.Firebase_Admin_SDK, self.Firestore_cllction_path,
                                  self.collection_empty_from_Sygic.to_dict(orient='records'), delete=True)

        # Write the new ones to FireBase
        Write_FireCloud_Documents(self.Firebase_Admin_SDK, self.Firestore_cllction_path,
                                  self.collection_empty_Google_updated)

        #Read the new ones from FireBase
        self.upd_collection = Read_FireCloud_Collection_Documents(self.Firebase_Admin_SDK, self.Firestore_cllction_path)

        return self.upd_collection

    def Update_Sygic_results(self):

        #Filter from the updated read collection only the results that Sygic did not initially returned results
        places_Sygic_returned = self.collection_return_from_Sygic['name'].tolist()
        filt_upd_collection = [i for i in self.upd_collection if i['name'] not in places_Sygic_returned]

        #Get the non empty recods from Sygic
        old_collection_Sygic = list(zip(self.collection_return_from_Sygic.index,
            [self.json_results[i] for i in self.collection_return_from_Sygic.index]))

        #Pass the new ones from SygicAPI again
        new_collection_Sygic = self.pass_collection_through_Sygic_places(new_collection=filt_upd_collection)

        #Pass the order number of the initially empty results
        new_collection_Sygic = list(zip(self.collection_empty_from_Sygic.index, new_collection_Sygic))

        #Merge them with the old ones from Sygic and sort the list by the original indexes
        self.new_collection_Sygic = sorted(old_collection_Sygic + new_collection_Sygic)

        return self.new_collection_Sygic

    def Write_Sygic_results_to_FireBase(self):

        #Include the Sygic Results to the dicts
        sygic_results_filtered = [i[1]['data']['places'][0] if i[1]['data']['places'] else
                  None for i in self.new_collection_Sygic]

        keys_to_keep = {'id', 'level', 'rating', 'rating_local', 'location', 'name', 'name_local', 'url',
                        'duration_estimate', 'marker', 'class', 'categories', 'tag_keys', 'parents'}

        sygic_results_filtered = [i if i is None else {key: i[key] for key in i.keys() & keys_to_keep}
                                  for i in sygic_results_filtered]

        Sygic_upd_collection = pd.concat([pd.DataFrame(self.upd_collection), pd.DataFrame(sygic_results_filtered)],
                                         axis=1).rename(
            columns={0: 'Sygic_Results'}).to_dict(orient='records')

        #Write all the results back to FireBase
        Write_FireCloud_Documents(self.Firebase_Admin_SDK, self.Firestore_cllction_path, Sygic_upd_collection)

def SygicPassPipeline(Firebase_Admin_SDK, Firestore_cllction_path, API_KEY, Sygic_API_KEY, country, city):

    sygicpassinst = SygicPass(Firebase_Admin_SDK, Firestore_cllction_path, API_KEY, Sygic_API_KEY, country, city)
    sygicpassinst.pass_collection_through_Sygic_places()
    sygicpassinst.eval_Sygic_results()
    sygicpassinst.Google_API_replace_with_most_popular()
    sygicpassinst.Update_FireBase()
    sygicpassinst.Update_Sygic_results()
    sygicpassinst.Write_Sygic_results_to_FireBase()

def SygicDetailsPipeline(Firebase_Admin_SDK, Firestore_cllction_path, Sygic_API_KEY, keys_list=None):

    # Read the collection from FireBase
    collection = Read_FireCloud_Collection_Documents(Firebase_Admin_SDK, Firestore_cllction_path)

    # Filter the documents for which Sygic Results is Not None
    collection = [(doc['FireBaseCloud_id'], doc['Sygic_Results']) for doc in collection if
                  doc['Sygic_Results'] is not None]
    for i, elem in enumerate(collection):
        collection[i][1].update({'FireBaseCloud_id': collection[i][0]})
    collection = [i[1] for i in collection]
    print(collection)

    # Pass the collection from the Sygic details API
    # Get Places Details
    url = 'https://api.sygictravelapi.com/1.2/en/places/'

    for doc in collection:
        search_place = doc['id']
        res = requests.get(url + search_place, headers={'x-api-key': Sygic_API_KEY})
        json_details = res.json()['data']['place']

        # Pick the desired keys
        if keys_list:
            json_details = {k: json_details[k] for k in keys_list}
        doc.update(json_details)

    return collection

















