import pandas as pd
import glob2
from pointage import adresses

files_datalake = glob2.glob('/home/jovyan/work/Crawls Groups Users/Listes/**/final_*.csv')
df_m = pd.read_csv(files_datalake[0], sep = '\t')
df_m['file'] = files_datalake[0].replace('/home/jovyan/work/Crawls Groups Users/Listes/','')
for file in files_datalake:
    df_i = pd.read_csv(file, sep = '\t')
    df_i['file'] = file.replace('/home/jovyan/work/Crawls Groups Users/Listes/','')
    df_m = pd.concat([df_m, df_i])


df_m['street_number'] = df_m.apply(lambda x: parse_adrs(x['formatted_address']).street_number, axis=1)
df_m['road'] = df_m.apply(lambda x: parse_adrs(x['formatted_address']).road, axis=1)
df_m['suburb'] = df_m.apply(lambda x: parse_adrs(x['formatted_address']).suburb, axis=1)
df_m['city'] = df_m.apply(lambda x: parse_adrs(x['formatted_address']).city, axis=1)
df_m['district'] = df_m.apply(lambda x: parse_adrs(x['formatted_address']).district, axis=1)
df_m['postcode'] = df_m.apply(lambda x: parse_adrs(x['formatted_address']).postcode, axis=1)
df_m['country'] = df_m.apply(lambda x: parse_adrs(x['formatted_address']).country, axis=1)

df_m.to_csv('/home/jovyan/consolidecrawl.csv', sep = '\t')
