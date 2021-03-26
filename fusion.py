from pointage import support as sp
from pointage import hotels as ho
from customsearch import geocode as gc
import os
import glob

def fusion(filename):
    result = glob.glob('*.csv')
    temp_result=[x for x in result if 'hotels16' not in x and 'final_' not in x]
    main_csv=temp_result[0]
    temp_result=[x for x in result if 'hotels16' in x]
    alter_csv=temp_result[0]
    main_pandas=sp.pd.read_csv(main_csv,sep='\t',encoding='utf-8')
    alter_pandas=sp.pd.read_csv(alter_csv,sep='\t',encoding='utf-8')
    alter_pandas=alter_pandas.rename(columns={'Hotel Name':'nom'})
    main_pandas['nom'] = main_pandas['nom'].astype(str)
    alter_pandas['nom'] = alter_pandas['nom'].astype(str)
    main_pandas['nom']= main_pandas['nom'].str.strip()
    alter_pandas['nom']= alter_pandas['nom'].str.strip()
    final_pandas=main_pandas.merge(alter_pandas, on='nom',how='left')
    final_pandas.capacité.fillna(final_pandas.Capacities, inplace=True)
    final_pandas.etoiles.fillna(final_pandas.stars, inplace=True)
    final_pandas['adress'].fillna(final_pandas['external_adresse'],inplace=True)
    del final_pandas['Capacities']
    del final_pandas['stars']
    final_pandas=final_pandas.rename(columns={'url_x':'url_original'})
    final_pandas=final_pandas.rename(columns={'webname':'external_name'})
    final_pandas=final_pandas.rename(columns={'address':'external_adresse'})
    final_pandas=final_pandas.rename(columns={'url_y':'external_url'})
    filenamexlsx='final_'+filename+'.xlsx'
    filenamecsv='final_'+filename+'.csv'
    final_pandas.to_excel(filenamexlsx, na_rep='', index=False)
    final_pandas.to_csv(filenamecsv, sep='\t',na_rep='', index=False)
