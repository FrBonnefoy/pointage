import os
import pandas as pd
import numpy as np
import pickle
import spacy
from tqdm.notebook import tqdm
import matplotlib.pyplot as plt
from gensim.models.fasttext import FastText
from rank_bm25 import BM25Okapi
import nmslib
import time
import jellyfish as js

print('Initializing search engine...\n')
df = pd.read_csv('~/parc2020.csv', sep = ';', encoding = 'latin')
df = df.astype(str)

nlp = spacy.load("en_core_web_sm")
tok_text=[] # for our tokenised corpus
#Tokenising using SpaCy:
for doc in tqdm(nlp.pipe(df.nom_commercial.str.lower().values, disable=["tagger", "parser","ner", "lemmatizer"])):
    tok = [t.text for t in doc if t.is_alpha]
    tok_text.append(tok)
bm25 = BM25Okapi(tok_text)

def flag_codex(x,y):
    rating_codex1 = [js.match_rating_codex(z) for z in x.split()]
    string_codex1 = ''.join(rating_codex1)
    rating_codex2 = [js.match_rating_codex(z) for z in y.split()]
    string_codex2 = ''.join(rating_codex2)
    score = js.jaro_winkler_similarity(string_codex1, string_codex2)
    return score

class matcher:
    def __init__(self,match):
        self.match = match
        try:
            self.id = df[df['nom_commercial']== self.match].id_hotel.iloc[0]
        except IndexError:
            self.id = 'Not Found'
        try:
            self.country = df[df['nom_commercial']== self.match].libelle_pays.iloc[0]
        except IndexError:
            self.country = 'Not Found'
        try:
            self.city = df[df['nom_commercial']== self.match].libelle_ville.iloc[0]
        except IndexError:
            self.city = 'Not Found'


class searchengine:
    def __init__(self,query):
        self.query = query
        self.tquery= self.query.lower().split(" ")

    def search(self,x=0):
        global results
        global results_codex
        global results_codex2
        global results_id
        global match
        results_codex = {}
        results_id = {}
        results = bm25.get_top_n(self.tquery, df.nom_commercial.values, n=5)
        for result in results:
            results_codex[result] = flag_codex(result,self.query)
        results_codex2={}
        for result in results_codex:
            if results_codex[result] > 0.5:
                results_codex2[result] = results_codex[result]
        try:
            match = max(results_codex2, key=results_codex2.get)
        except ValueError:
            match = 'Not Found'
        for result in results:
            results_id[result] = matcher(result).id
        if x==1:
            return results_codex
        elif x==2:
            return results_id
        else:
            return match
