from pointage import support as sp
from pointage import hotels as ho
from customsearch import geocode as gc
import os
import glob

class branding():
    def __init__(self,x,y):
        self.candidates=x
        self.name=y
        self.brand=None
        for z in self.candidates:
            if z.lower() in self.name.lower():
                self.brand=z

def fusion(filename,brands):
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


    del final_pandas['Capacities']
    del final_pandas['stars']
    final_pandas=final_pandas.rename(columns={'url_x':'url_original'})
    final_pandas=final_pandas.rename(columns={'webname':'external_name'})
    final_pandas=final_pandas.rename(columns={'address':'external_adresse'})
    final_pandas=final_pandas.rename(columns={'url_y':'external_url'})
    final_pandas['adress'].fillna(final_pandas['external_adresse'],inplace=True)
    
    brand_pandas=final_pandas['nom']
    brand_pandas=brand_pandas.to_frame()
    brand_pandas['BRAND'] = brand_pandas.apply(lambda x: branding(brands,x['nom']).brand, axis=1)

    final_pandas=final_pandas.merge(brand_pandas, on='nom',how='left')

    geo_pandas=final_pandas['adress']
    geo_pandas=geo_pandas.to_frame()
    geo_pandas['data'] = geo_pandas.apply(lambda x: gc.searcher(x['adress']).data, axis=1)
    geo_pandas['street_number'] = geo_pandas.apply(lambda x: gc.parser(x['data']).street_number, axis=1)
    geo_pandas['neighborhood'] = geo_pandas.apply(lambda x: gc.parser(x['data']).neighborhood, axis=1)
    geo_pandas['locality'] = geo_pandas.apply(lambda x: gc.parser(x['data']).locality, axis=1)
    geo_pandas['aa2'] = geo_pandas.apply(lambda x: gc.parser(x['data']).aa2, axis=1)
    geo_pandas['aa1'] = geo_pandas.apply(lambda x: gc.parser(x['data']).aa1, axis=1)
    geo_pandas['country'] = geo_pandas.apply(lambda x: gc.parser(x['data']).country, axis=1)
    geo_pandas['code_postal'] = geo_pandas.apply(lambda x: gc.parser(x['data']).code_postal, axis=1)
    geo_pandas['lat'] = geo_pandas.apply(lambda x: gc.parser(x['data']).lat, axis=1)
    geo_pandas['lng'] = geo_pandas.apply(lambda x: gc.parser(x['data']).lng, axis=1)
    geo_pandas['bounds'] = geo_pandas.apply(lambda x: gc.parser(x['data']).bounds, axis=1)
    geo_pandas['viewport'] = geo_pandas.apply(lambda x: gc.parser(x['data']).viewport, axis=1)
    geo_pandas['formatted_address'] = geo_pandas.apply(lambda x: gc.parser(x['data']).faddress, axis=1)
    geo_pandas['id'] = geo_pandas.apply(lambda x: gc.parser(x['data']).id, axis=1)
    geo_pandas['UE'] = geo_pandas.apply(lambda x: gc.parser(x['data']).ue, axis=1)
    del geo_pandas['data']

    final_pandas=final_pandas.merge(geo_pandas, on='adress',how='left')


    filenamexlsx='final_'+filename+'.xlsx'
    filenamecsv='final_'+filename+'.csv'
    final_pandas.to_excel(filenamexlsx, na_rep='', index=False)
    final_pandas.to_csv(filenamecsv, sep='\t',na_rep='', index=False)
