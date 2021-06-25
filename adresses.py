from customsearch import geocode as gc
import os
import glob
import time
from tqdm.notebook import tqdm
import numpy as np
import pandas as pd
from postal.expand import expand_address
from postal.parser import parse_address

def send_codex(x):
    rating_codex1 = [js.match_rating_codex(y) for y in x.split()]
    return ''.join(rating_codex1)


def obtain(x):
    try:
        df_input = pd.read_excel(x)
    except:
        with open(x) as f:
            input_ = f.readlines()
            input_ = [x.strip() for x in input_]
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

    for a_data in tqdm(df_list):
        time.sleep(1)
        a_data['DATA'] = a_data.apply(lambda x: gc.searcher_detail(x[a_data.columns[0]]).data, axis=1)
        a_data['PLACE_ID'] = a_data.apply(lambda x: gc.searcher_detail(x[a_data.columns[0]]).place_id, axis=1)
        a_data['NAME'] = a_data.apply(lambda x: gc.searcher_detail(x[a_data.columns[0]]).name, axis=1)
        a_data['ADRS'] = a_data.apply(lambda x: gc.searcher_detail(x[a_data.columns[0]]).adrs, axis=1)
        a_data['street_number'] = a_data.apply(lambda x: gc.searcher_detail(x[a_data.columns[0]]).street_number, axis=1)
        a_data['road'] = a_data.apply(lambda x: gc.searcher_detail(x[a_data.columns[0]]).road, axis=1)
        a_data['suburb'] = a_data.apply(lambda x: gc.searcher_detail(x[a_data.columns[0]]).suburb, axis=1)
        a_data['city'] = a_data.apply(lambda x: gc.searcher_detail(x[a_data.columns[0]]).city, axis=1)
        a_data['district'] = a_data.apply(lambda x: gc.searcher_detail(x[a_data.columns[0]]).district, axis=1)
        a_data['postcode'] = a_data.apply(lambda x: gc.searcher_detail(x[a_data.columns[0]]).postcode, axis=1)
        a_data['country'] = a_data.apply(lambda x: gc.searcher_detail(x[a_data.columns[0]]).country, axis=1)


    df_input = pd.concat(df_list,ignore_index=True)

    return df_input


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
