#Import modules

from customsearch import geocode as gc
import os
import glob
import time
from tqdm.notebook import tqdm
import numpy as np
import pandas as pd
from postal.expand import expand_address
from postal.parser import parse_address
import jellyfish as js

# Codifies names according to phonetics

def send_codex(x):
    rating_codex1 = [js.match_rating_codex(y) for y in x.split()]
    return ''.join(rating_codex1)

# Flags strings that are very dissimilar

def flag_detail(s1,s2):
    try:
        if jellyfish.jaro_winkler_similarity(s1,s2) > 0.7:
            return 'OK'
        else:
            return 'CHECK'
    except:
        return None

# Returns a dictionary with the number of occurences for each item of a given list

def CountFrequency(my_list):

    # Creating an empty dictionary
    freq = {}
    for items in my_list:
        freq[items] = my_list.count(items)

    return freq

# Obtains MKG IDs for a given list of hotel properties, the names of the properties must be on the first column

def obtain(x):

    # Read the file
    try:
        df_input = pd.read_excel(x)
    except:
        with open(x) as f:
            input_ = f.readlines()
            input_ = [x.strip() for x in input_]
            df_input = pd.DataFrame(input_)

    # Divides the dataframe into chunks so the API does not get overwhelmed
    factors=[]
    number=len(df_input.index)
    for whole_number in range(1, number + 1):
        if number % whole_number == 0:
            factors.append(whole_number)

    filtered=[x for x in factors if (number/x)<=10]
    chunk=min(filtered)
    df_list = np.vsplit(df_input, chunk)

    # Passes each chunk of the dataframe through the searcher_detail function of the geocode module. It will add additional columns to the dataframe, where various google search attributes can be found for each name on the dataframe.
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

    # It joins every chunk back together into the main dataframe.
    df_input = pd.concat(df_list,ignore_index=True)
    # Uses the flag_detail function to see if the Google Search sent accurate results.
    df_input['CHECK_INFO_GOOGLE'] = df_input.apply(lambda x: flag_detail(x[df_input.columns[0]],x['NAME']), axis=1)
    # Reads the file with the information on the worldwide chain hotel supply. This line of code, and the corresponding file, should be updated each year.
    df_parc = pd.read_pickle('/home/jovyan/parc2020')

    # Creates extra columns where possible matches can be stored. Two methods are used: string similarity and address matching.
    df_input['ID_MATCH_NAME'] = pd.Series( dtype="string")
    df_input['NAME_MATCH_NAME'] = pd.Series( dtype="string")
    df_input['ID_MATCH_ADRS'] = pd.Series( dtype="string")
    df_input['NAME_MATCH_ADRS'] = pd.Series( dtype="string")

    # It assures that all names on the main dataframe are strings
    df_parc['nom_commercial'] = df_parc['nom_commercial'].astype(str)

    # It phoenetically encodes the names on the input and on the main dataframe
    df_parc['CODEX_MKG'] = df_parc.apply(lambda x: send_codex(x['nom_commercial']), axis=1)
    df_input['CODEX_LIST'] = df_input.apply(lambda x: send_codex(x[df_list.columns[0]]), axis=1)

    # It starts to fill the cells of the input
    for z in tqdm(range(len(df_input))):

        try:
            del df_parc['JARO']
        except:
            pass

        # A new temporary column is created on the dataframe with each iteration. This column calculates the string similarity of cell with all the cells on the main dataframe.
        df_parc['JARO'] = df_parc.apply(lambda x: js.jaro_winkler_similarity(df_input.iloc[z]['CODEX_LIST'], x['CODEX_MKG']), axis=1)
        # Matches to the most similar cell on the main dataframe
        match = df_parc.loc[df_parc['JARO'].idxmax()]
        # If these names are sufficiently similar, the name and id are taken to the input.
        if match['JARO']>0.7:
            df_input.at[z,'ID_MATCH_NAME'] = str(match['id_hotel'])
            df_input.at[z,'NAME_MATCH_NAME'] = str(match['nom_commercial'])

        # Starts the process of matching by adress. First, country; second, city; third, road; fourth, street number.
        if len(df_input.iloc[z]['country'])>0:
            df_parc_filter1 = df_parc[df_parc['COUNTRY']==df_input.iloc[z]['country']]
            if len(df_input.iloc[z]['city'])>0:
                df_parc_filter2 = df_parc_filter1[df_parc_filter1['CITY']==df_input.iloc[z]['city']]
                if len(df_input.iloc[z]['road'])>0:
                    df_parc_filter3 = df_parc_filter2[df_parc_filter2['ROAD']==df_input.iloc[z]['road']]
                    if len(df_input.iloc[z]['street_number'])>0:
                        df_parc_filter4 = df_parc_filter3[df_parc_filter3['STREET_NUMBER']==df_input.iloc[z]['street_number']]
                        df_parc_final = df_parc_filter3
                else:
                    df_parc_final = df_parc_filter3
            else:
                df_parc_final = df_parc_filter2
        else:
            df_parc_final = df_parc_filter1

        # If a match is found, then they are transcribed on the input
        if len(df_parc_final) > 0:
            df_input.at[z,'ID_MATCH_ADRS'] = str(df_parc_final['id_hotel'])
            df_input.at[z,'NAME_MATCH_ADRS'] = str(df_parc_final['nom_commercial'])
        else:
            df_input.at[z,'ID_MATCH_ADRS'] = ""
            df_input.at[z,'NAME_MATCH_ADRS'] = ""


    # It checks on the finalized input if there are name matches that occur more than once, it then flags them.

    df_input['CHECK_NAME_MATCH'] = pd.Series( dtype="int64")
    id_list = df_input['ID_MATCH_NAME'].to_list()
    id_list = [str(x) for x in id_list]
    try:
        id_list.remove('<NA>')
    except:
        pass

    id_sum = CountFrequency(id_list)

    for z in range(len(df_input)):
        try:
            if id_sum[df_input.iloc[z]['ID_MATCH_NAME']]>1:
                df_input.at[z, 'CHECK_NAME_MATCH'] = 1
            else:
                df_input.at[z, 'CHECK_NAME_MATCH'] = 0
        except:
            pass

    # Returns transformed input
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
