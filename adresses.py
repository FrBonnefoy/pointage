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
import concurrent.futures

# Codifies names according to phonetics

def send_codex(x):
    rating_codex1 = [js.match_rating_codex(y) for y in x.split()]
    return ''.join(rating_codex1)

# Flags strings that are very dissimilar

def flag_detail(s1,s2):
    try:
        if js.jaro_winkler_similarity(s1,s2) > 0.9:
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

# Flags if matched ID has any rank 2 present
def rank2_flag(x):
    try:
        filtered_rank = df_parc[df_parc['id_hotel']==x]
        if len(filtered_rank[filtered_rank['rang']>1]):
            return 'YES'
        else:
            return 'NO'
    except:
        return None


# Initializes main dataframe
def init_parc():
    global df_parc
    # Reads the file with the information on the worldwide chain hotel supply. This line of code, and the corresponding file, should be updated each year.
    df_parc = pd.read_pickle('/home/jovyan/parc2020')
    # It assures that all names and ids on the main dataframe are strings
    df_parc['nom_commercial'] = df_parc['nom_commercial'].astype(str)
    df_parc['id_hotel'] = df_parc['id_hotel'].astype(str)
    df_parc['rang'] = df_parc['rang'].fillna(0)
    df_parc['rang'] = df_parc['rang'].astype('int64')
    # It phoenetically encodes the names on the input and on the main dataframe
    df_parc['CODEX_MKG'] = df_parc.apply(lambda x: send_codex(x['nom_commercial']), axis=1)
    del df_parc['telephone']
    del df_parc['e_mail']
    del df_parc['id_chaine']
    del df_parc['id_groupe_hotelier']
    del df_parc['MDG 2020']
    del df_parc['type de site']
    del df_parc['Gamme enseigne']
    del df_parc['Gamme 2']
    del df_parc['Enseigne 2']
    del df_parc['New enseigne']
    del df_parc['agglo n-1']
    del df_parc['étoile réelle n-1']
    del df_parc['étoile MKG n-1']
    del df_parc['capacité n-1']
    del df_parc['Type n-1']
    del df_parc['enseigne n-1']
    del df_parc['Validation 2018']
    del df_parc['MDG 2019']
    del df_parc['Pays n-1']
    del df_parc['Région n-1']
    del df_parc['Code Import']

# Function that matches to the main dataframe names and IDs, according to name and address matching, for a given row of input

def row_match(z):
    global df_input
    df_parc2 = df_parc
    # It drops the Jaro column if necessary
    try:
        del df_parc2['JARO']
    except:
        pass

    # A new temporary column is created on the dataframe with each iteration. This column calculates the string similarity of cell with all the cells on the main dataframe.
    df_parc2['JARO'] = df_parc2.apply(lambda x: js.jaro_winkler_similarity(df_input.iloc[z]['CODEX_LIST'], x['CODEX_MKG']), axis=1)
    # Matches to the most similar cell on the main dataframe
    match = df_parc2.loc[df_parc2['JARO'].idxmax()]
    # If these names are sufficiently similar, the name and id are taken to the input.
    if match['JARO']>0.8:
        df_input.at[z,'ID_MATCH_NAME'] = str(match['id_hotel'])
        df_input.at[z,'NAME_MATCH_NAME'] = str(match['nom_commercial'])

    # Starts the process of matching by adress. First, country; second, city; third, road; fourth, street number.
    df_parc_filter1 = df_parc2[df_parc2['COUNTRY']==df_input.iloc[z]['country']]
    df_parc_filter2 = df_parc_filter1[df_parc_filter1['CITY']==df_input.iloc[z]['city']]
    df_parc_filter3 = df_parc_filter2[df_parc_filter2['ROAD']==df_input.iloc[z]['road']]
    df_parc_filter4 = df_parc_filter3[df_parc_filter3['STREET_NUMBER']==df_input.iloc[z]['street_number']]
    df_parc_final = df_parc_filter4


    # If a match is found, then they are transcribed on the input
    if len(df_parc_final) > 0:
        df_input.at[z,'ID_MATCH_ADRS'] = str(df_parc_final.iloc[0]['id_hotel'])
        df_input.at[z,'NAME_MATCH_ADRS'] = str(df_parc_final.iloc[0]['nom_commercial'])
    else:
        df_input.at[z,'ID_MATCH_ADRS'] = ""
        df_input.at[z,'NAME_MATCH_ADRS'] = ""



# Obtains MKG IDs for a given list of hotel properties, the names of the properties must be on the first column
def obtain(x):
    #Initializes main dataframe
    init_parc()
    # Reads the file
    global df_input
    if 'final_' in x:
        try:
            df_input = pd.read_excel(x, sheet_name = 'DATA')
        except:
            df_input = pd.read_excel(x)

        nom = df_input["nom"]
        df_input.drop(labels=["nom"], axis=1,inplace = True)
        df_input.insert(0, "nom", nom)
    else:
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
        time.sleep(0.75)
        a_data['DATA'] = a_data.apply(lambda x: gc.searcher_detail(x[a_data.columns[0]]).data, axis=1)


    # It joins every chunk back together into the main dataframe.
    df_input = pd.concat(df_list,ignore_index=True)

    # It parses the column data into new columns
    df_input['PLACE_ID'] = df_input.apply(lambda x: gc.parser_detail(x['DATA']).place_id, axis=1)
    df_input['NAME'] = df_input.apply(lambda x: gc.parser_detail(x['DATA']).name, axis=1)
    df_input['ADRS'] = df_input.apply(lambda x: gc.parser_detail(x['DATA']).adrs, axis=1)
    df_input['street_number'] = df_input.apply(lambda x: gc.parser_detail(x['DATA']).street_number, axis=1)
    df_input['road'] = df_input.apply(lambda x: gc.parser_detail(x['DATA']).road, axis=1)
    df_input['suburb'] = df_input.apply(lambda x: gc.parser_detail(x['DATA']).suburb, axis=1)
    df_input['city'] = df_input.apply(lambda x: gc.parser_detail(x['DATA']).city, axis=1)
    df_input['district'] = df_input.apply(lambda x: gc.parser_detail(x['DATA']).district, axis=1)
    df_input['postcode'] = df_input.apply(lambda x: gc.parser_detail(x['DATA']).postcode, axis=1)
    df_input['country'] = df_input.apply(lambda x: gc.parser_detail(x['DATA']).country, axis=1)

    # Uses the flag_detail function to see if the Google Search sent accurate results.
    df_input['NAME'] = df_input['NAME'].astype(str)
    df_input[df_input.columns[0]] = df_input[df_input.columns[0]].astype(str)
    df_input['CHECK_INFO_GOOGLE'] = df_input.apply(lambda x: flag_detail(x[df_input.columns[0]],x['NAME']), axis=1)

    # Creates extra columns where possible matches can be stored. Two methods are used: string similarity and address matching.
    df_input['ID_MATCH_NAME'] = pd.Series( dtype="string")
    df_input['NAME_MATCH_NAME'] = pd.Series( dtype="string")
    df_input['ID_MATCH_ADRS'] = pd.Series( dtype="string")
    df_input['NAME_MATCH_ADRS'] = pd.Series( dtype="string")

    # It phoenetically encodes the names on the input
    df_input['CODEX_LIST'] = df_input.apply(lambda x: send_codex(x[df_input.columns[0]]), axis=1)

    # It launches parallel processing of row_match function through dataframe

    list_row = list(range(len(df_input)))

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    	future_to_row = {executor.submit(row_match, row): row for row in list_row}
    	for future in tqdm(concurrent.futures.as_completed(future_to_row),total=len(list_row)):
    		row = future_to_row[future]
    		try:
    			data = future.result()
    		except Exception as exc:
    			print(exc)


    # It checks on the finalized input if there are name matches that occur more than once, it then flags them.
    df_input['CHECK_NAME_MATCH'] = pd.Series( str )
    id_list = df_input['ID_MATCH_NAME'].to_list()
    id_list = [str(x) for x in id_list]
    id_list = df_input[df_input['ID_MATCH_NAME'].notnull()]['ID_MATCH_NAME'].to_list()
    try:
        id_list.remove('<NA>')
    except:
        pass

    id_sum = CountFrequency(id_list)

    for z in range(len(df_input)):
        try:
            if id_sum[df_input.iloc[z]['ID_MATCH_NAME']]>1:
                df_input.at[z, 'CHECK_NAME_MATCH'] = 'CHECK'
            else:
                df_input.at[z, 'CHECK_NAME_MATCH'] = 'OK'
        except:
            pass

    #It consolidates all results on a single column and puts it at the second place

    df_input['ID_MKG_MATCH'] = df_input['ID_MATCH_ADRS']
    df_input.loc[df_input["ID_MKG_MATCH"].isnull(),'ID_MKG_MATCH'] = df_input['ID_MATCH_NAME']
    df_input.loc[df_input["ID_MKG_MATCH"] == '','ID_MKG_MATCH'] = df_input['ID_MATCH_NAME']
    mkg_id = df_input["ID_MKG_MATCH"]
    df_input.drop(labels=["ID_MKG_MATCH"], axis=1,inplace = True)
    df_input.insert(1, "ID_MKG_MATCH", mkg_id)

    # It checks if there are rank 2 in the transformed dataframe
    df_input['HAS_RANK2'] = df_input.apply(lambda x: rank2_flag(x['ID_MKG_MATCH']), axis=1)

    # Returns transformed input
    return df_input
    df_input.to_pickle('temp_df_input')



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
