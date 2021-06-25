from customsearch import geocode as gc
import os
import glob
import time
from tqdm.notebook import tqdm
import numpy as np
import pandas as pd

def obtain(x):
    try:
        df_input = pd.read_excel(x)
    except:
        with open(x) as f:
            input_ = f.readlines()
            df_input = pd.DataFrame(input_)

    factors=[]
    number=len(df_input.index)
    for whole_number in range(1, number + 1):
        if number % whole_number == 0:
            factors.append(whole_number)

    filtered=[x for x in factors if (number/x)<=10]
    chunk=min(filtered)
    df_list = np.vsplit(df_input, chunk)

    print('Fetching location data...')

    for a_data in tqdm(geo_list):
        time.sleep(1)
        a_data['data'] = a_data.apply(lambda x: gc.searcher(x['adress']).data, axis=1)

    df_input = pd.concat(geo_list,ignore_index=True)

    df_input['street_number'] = df_input.apply(lambda x: gc.parser(x['data']).street_number, axis=1)
    df_input['route'] = df_input.apply(lambda x: gc.parser(x['data']).route, axis=1)
    df_input['neighborhood'] = df_input.apply(lambda x: gc.parser(x['data']).neighborhood, axis=1)
    df_input['locality'] = df_input.apply(lambda x: gc.parser(x['data']).locality, axis=1)
    df_input['aa2'] = df_input.apply(lambda x: gc.parser(x['data']).aa2, axis=1)
    df_input['aa1'] = df_input.apply(lambda x: gc.parser(x['data']).aa1, axis=1)
    df_input['country'] = df_input.apply(lambda x: gc.parser(x['data']).country, axis=1)
    df_input['code_postal'] = df_input.apply(lambda x: gc.parser(x['data']).code_postal, axis=1)
    df_input['lat'] = df_input.apply(lambda x: gc.parser(x['data']).lat, axis=1)
    df_input['lng'] = df_input.apply(lambda x: gc.parser(x['data']).lng, axis=1)
    df_input['bounds'] = df_input.apply(lambda x: gc.parser(x['data']).bounds, axis=1)
    df_input['viewport'] = df_input.apply(lambda x: gc.parser(x['data']).viewport, axis=1)
    df_input['formatted_address'] = df_input.apply(lambda x: gc.parser(x['data']).faddress, axis=1)
    df_input['id'] = df_input.apply(lambda x: gc.parser(x['data']).id, axis=1)
    df_input['UE'] = df_input.apply(lambda x: gc.parser(x['data']).ue, axis=1)


class parse_adrs:
    def __init__(self,query):
        self.query = query
        self.data = parse_address(query)

        try:
            street_number = list(filter(lambda x: x[1]=='house_number', self.data))[0][0]
        except:
            street_number = ''

        try:
            road = list(filter(lambda x: x[1]=='road', self.data))[0][0]
        except:
            road = ''

        try:
            suburb = list(filter(lambda x: x[1]=='suburb', self.data))[0][0]
        except:
            suburb = ''

        try:
            city = list(filter(lambda x: x[1]=='city', self.data))[0][0]
        except:
            city = ''

        try:
            district = list(filter(lambda x: x[1]=='state_district', self.data))[0][0]
        except:
            district = ''

        try:
            postcode = list(filter(lambda x: x[1]=='postcode', self.data))[0][0]
        except:
            postcode = ''

        try:
            country = list(filter(lambda x: x[1]=='country', self.data))[0][0]
        except:
            country = ''

        self.street_number = street_number
        self.road = road
        self.suburb = suburb
        self.city = city
        self.district = district
        self.postcode = postcode
        self.country = country

class parse_adrs_norm:
    def __init__(self,query):
        self.query = query
        self.data = parse_address(expand_address(query)[0])

        try:
            street_number = list(filter(lambda x: x[1]=='house_number', self.data))[0][0]
        except:
            street_number = ''

        try:
            road = list(filter(lambda x: x[1]=='road', self.data))[0][0]
        except:
            road = ''

        try:
            suburb = list(filter(lambda x: x[1]=='suburb', self.data))[0][0]
        except:
            suburb = ''

        try:
            city = list(filter(lambda x: x[1]=='city', self.data))[0][0]
        except:
            city = ''

        try:
            district = list(filter(lambda x: x[1]=='state_district', self.data))[0][0]
        except:
            district = ''

        try:
            postcode = list(filter(lambda x: x[1]=='postcode', self.data))[0][0]
        except:
            postcode = ''

        try:
            country = list(filter(lambda x: x[1]=='country', self.data))[0][0]
        except:
            country = ''

        self.street_number = street_number
        self.road = road
        self.suburb = suburb
        self.city = city
        self.district = district
        self.postcode = postcode
        self.country = country
