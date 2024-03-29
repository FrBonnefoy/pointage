#Import modules
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as soup
import time
import logging
import os
from tqdm.notebook import tqdm
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import concurrent.futures
import concurrent.futures
import requests
import re
import json
import pandas as pd
import datetime
import glob
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from pointage import support as sp
from random import randint
from customsearch import customsearch as cs
import jellyfish
import urllib


def flag(s1,s2):
    try:
        if jellyfish.jaro_winkler_similarity(s1,s2) > 0.7:
            return "OK"
        else:
            return "CHECK"
    except:
        return "NaN"


#Define open_session

def open_session_firefox():
    global browser
    PROXY="127.0.0.1:24001"
    webdriver.DesiredCapabilities.FIREFOX['proxy'] = {
    "httpProxy": PROXY,
    "ftpProxy": PROXY,
    "sslProxy": PROXY,
    "proxyType": "MANUAL",
    }
    options = FirefoxOptions()
    options.add_argument('--proxy-server=%s' % PROXY)
    options.add_argument("--headless")
    options.add_argument("--width=1920")
    options.add_argument("--height=1080")
    #options.add_argument('start-maximized')
    profile = webdriver.FirefoxProfile()
    #profile.add_extension(current_path+"/disable_webrtc-1.0.23-an+fx.xpi")
    #profile.add_extension(current_path+"/adblock_for_firefox-4.24.1-fx.xpi")
    #profile.add_extension(current_path+"/image_block-5.0-fx.xpi")
    #profile.add_extension(current_path+"/ublock_origin-1.31.0-an+fx.xpi")
    profile.DEFAULT_PREFERENCES['frozen']["media.peerconnection.enabled" ] = False
    profile.set_preference("media.peerconnection.enabled", False)
    profile.set_preference("permissions.default.image", 2)
    profile.update_preferences()
    browser = webdriver.Firefox(profile,options=options)
    browser.set_window_size(1920, 1080)
    #browser.install_addon(current_path+"/disable_webrtc-1.0.23-an+fx.xpi", temporary=True)
    #browser.install_addon(current_path+"/image_block-5.0-fx.xpi", temporary=True)
    #browser.install_addon(current_path+"/ublock_origin-1.31.0-an+fx.xpi", temporary=True)

#Define close_session

def close_session():
    browser.close()

def pointer4_0(x):
    ts = time.gmtime()
    tsx=time.strftime("%s", ts)
    namefile='hotels'+'16_fast_'+'.csv'
    fhandle=open(namefile,'w', encoding="utf-8")
    headers = ("Hotel Name"+"\t"+'stars'+'\t'+"Capacities" + '\t' + "webname" + '\t' + "address" +'\t'+"url\n")
    fhandle.write(headers)
    fhandle.close()
    lines = open(x, 'r').readlines()
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
    	future_to_url = {executor.submit(scrape_hotel_info_2, url): url for url in lines}
    	for future in tqdm(concurrent.futures.as_completed(future_to_url),total=len(lines)):
    		url = future_to_url[future]
    		try:
    			data = future.result()
    		except Exception as exc:
    			with open('exception.txt',"a") as flog:
    				print('%r generated an exception: %s' % (url, exc),file=flog)
    		else:
    			with open('completed.txt',"a") as flog:
    				print('%r page is completed' % url,file=flog)



def scrape_hotel_info_2(x):
    y='hotels'+'16_fast_'+'.csv'
    time.sleep(3)
    x=x.strip().replace('"','')
    cosito=cs.custom_search(x)
    try:
        cosito.request()
    except Exception as e:
        pass

    #1st attempt hotels.com


    #print('checking hotels.com...')
    try:
        url=cosito.hotels
    except:
        url=''

    try:
        try:
            sp.req(url)
            webpage=sp.page.text
            toy_soup2 = soup(webpage, "html.parser")
            gold=toy_soup2.find("div",{"id":"overview-section-4"})
            if len(gold)<1:
                gold=toy_soup2.find("div",{"class":"_2cVsY2"})
            gold = str(gold)
            chambres = re.findall(r"(\d+) chambres", gold)
            if len(chambres)<1:
                chambres = re.findall(r"(\d+) appartements", gold)
            try:
                chambres=chambres[0]
            except:
                chambres=''
            try:
                sp.browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
            except:
                pass
            silver=toy_soup2.find("span",{"class":"star-rating-text star-rating-text-strong widget-star-rating-overlay widget-tooltip widget-tooltip-responsive widget-tooltip-ignore-touch"})
            if len(silver)<1:
                silver=toy_soup2.find("span",{"class":"_2dOcxA"})
            silver2=toy_soup2.find("span",{"class":"star-rating-text widget-star-rating-overlay widget-tooltip widget-tooltip-responsive widget-tooltip-ignore-touch"})
            try:
                silver = str(silver)
                silver2 = str(silver2)
                stars = re.findall(r"(\d?\,?\d) étoiles", silver)
                stars2 = re.findall(r"(\d?\,?\d) étoiles", silver2)
                if len(stars)==0 and len(stars2)>0:
                    stars=stars2[0]
                else:
                    stars=stars[0]
            except:
                stars=''
            bronze = toy_soup2.find("h2")
            if len(bronze)<1:
                bronze = toy_soup2.find("div",{'class':'_2h6Jhd'})
            try:
                bronze=str(bronze)
                bronze=bronze.replace("<h2>","")
                bronze=bronze.replace("</h2>","")
                vname = bronze
            except:
                vname = ""
            try:
                sp.browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
            except:
                pass
            plastic = toy_soup2.find("span",{"class":"postal-addr"})
            if len(plastic)<1:
                plastic = toy_soup2.findAll("span",{"class":"_2wKxGq _1clhIX"})
            try:
                adrs = plastic.text
            except:
                adrs = ""

        except:
            vname=""

        #Check if hotels.com returned valid information

        if flag(x,vname)=='OK':

            varlist=[str(x).replace('\t',''),str(stars).replace('\t',''),str(chambres).replace('\t',''),str(vname).replace('\t',''),str(adrs).replace('\t',''),str(url).replace('\t','')]
            to_append=varlist
            s = pd.DataFrame(to_append).T
            s.to_csv(y, mode='a', header=False,sep='\t',index=False)
            #time.sleep(1)
            #pbar.update(1)

        else:

            #Second attempt
            #print('checking hrs.com...')
            try:
                url=cosito.hrs
            except:
                url=''

            try:
                sp.req(url)

                description_rating=sp.scrape_light('span',{'class':'product--rating'})
                lectura_rating=description.now()
                sopa_stars=sp.soup(str(lectura_rating),'html.parser')
                stars=sopa_stars.findAll('i',{'class':'icon--star'})
                stars=str(len(stars))

                description_feature=sp.scrape_light('p',{'class':'feature'})
                lectura_feature=description_feature.now()
                regex=re.compile('chambres (\d+)')
                try:
                    chambres=regex.findall(str(lectura_feature[0]))[0]
                except:
                    chambres=""

                description_vname=sp.scrape_light('h1',{'class':'product--title'})
                lecture_vname=description_vname.now()
                try:
                    vname=lecture_vname[0].text.strip()
                except:
                    vname=""

                description_adrs=sp.scrape_light('span',{'class':'location-marker'})
                lecture_adrs=description_adrs.now()
                try:
                    adrs=' '.join(lecture_adrs[0].text.replace('\n','').split())
                except:
                    adrs=""

            except:
                vname=""

            if flag(x,vname)=='OK':

                varlist=[str(x).replace('\t',''),str(stars).replace('\t',''),str(chambres).replace('\t',''),str(vname).replace('\t',''),str(adrs).replace('\t',''),str(url).replace('\t','')]
                to_append=varlist
                s = pd.DataFrame(to_append).T
                s.to_csv(y, mode='a', header=False,sep='\t',index=False)
                #time.sleep(1)
                #pbar.update(1)

            else:

                #Third attempt

                #print('checking tripadvisor...')

                #Getting url
                try:
                    url=cosito.tripadvisor
                except:
                    url=''
                try:
                    sp.req(url)
                    webpage=sp.page.text
                    trip_soup = soup(webpage, "html.parser")
                    stars_trip=trip_soup.findAll('svg',{'class':'AZd6Ff4E'})
                    capacity_trip=trip_soup.findAll('div',{'id':'ABOUT_TAB'})
                    name_trip=trip_soup.findAll('h1',{'id':'HEADING'})
                    adrs_trip=trip_soup.findAll('span',{'class':'_3ErVArsu jke2_wbp'})

                    try:
                        stars=str(stars_trip[0]['title']).replace(' sur 5\xa0bulles','')
                    except:
                        stars=''
                    try:
                        chambres=str(capacity_trip)
                        roomsy=re.compile('NOMBRE DE CHAMBRES<\/div><div class="_1NHwuRzF">(\d+)')
                        chambres=roomsy.findall(chambres)[0]
                    except:
                        chambres=''
                    try:
                        vname=name_trip[0].text
                    except:
                        vname=''
                    try:
                        adrs=adrs_trip[0].text
                    except:
                        adrs=""
                except:
                    vname=""
                #Check if tripadvisor returned valid information

                if flag(x,vname)=='OK':

                    varlist=[str(x).replace('\t',''),str(stars).replace('\t',''),str(chambres).replace('\t',''),str(vname).replace('\t',''),str(adrs).replace('\t',''),str(url).replace('\t','')]
                    to_append=varlist
                    s = pd.DataFrame(to_append).T
                    s.to_csv(y, mode='a', header=False,sep='\t',index=False)
                    #time.sleep(1)
                    #pbar.update(1)

                else:
                    stars=''
                    chambres=''
                    vname=''
                    adrs=''
                    url=''
                    #print(x, 'could not be completed','because of',' not found')
                    varlist=[x,stars,chambres,vname,adrs,url]
                    to_append=varlist
                    s = pd.DataFrame(to_append).T
                    s.to_csv(y, mode='a', header=False,sep='\t',index=False)
                    #time.sleep(1)
                    #pbar.update(1)

    except Exception as ex:
        stars=''
        chambres=''
        vname=''
        adrs=''
        url=''
        #print(x, 'could not be completed','because of',ex)
        varlist=[x,stars,chambres,vname,adrs,url]
        to_append=varlist
        s = pd.DataFrame(to_append).T
        s.to_csv(y, mode='a', header=False,sep='\t',index=False)
        #time.sleep(1)
        #pbar.update(1)






def pointer3_0(x):
    global pbar
    ts = time.gmtime()
    tsx=time.strftime("%s", ts)
    namefile='hotels'+tsx+'.csv'
    fhandle=open(namefile,'w', encoding="utf-8")
    headers = ("Hotel Name"+"\t"+'stars'+'\t'+"Capacities" + '\t' + "webname" + '\t' + "address" +'\t'+"url\n")
    fhandle.write(headers)
    fhandle.close()
    lines = open(x, 'r').readlines()
    pbar=tqdm(total=len(lines))
    for line in lines:
        time.sleep(randint(2,5))
        print(line.strip())
        scrape_hotel_info(line,namefile)
     #booking_files = glob.glob(namefile)
    pcsv=pd.read_csv(namefile, sep = '\t')
    pcsv['flag_pointage'] = pcsv.apply(lambda x: flag(x['nom'], x['external_name']) , axis=1 )
    pcsv = pcsv.insert(0, 'flag_pointage_', pcsv['flag_pointage'])
    del pcsv['flag_pointage']
    pcsv = pcsv.rename(columns={'flag_pointage_':'flag_pointage'})
    pcsv.to_csv(filename, sep = '\t', index = False)

def scrape_hotel_info(x,y):
    x=x.strip().replace('"','')
    cosito=cs.custom_search(x)
    try:
        cosito.request()
    except Exception as e:
        print(e)

    #1st attempt hotels.com


    print('checking hotels.com...')
    try:
        url=cosito.hotels
    except:
        url=''

    try:
        try:
            sp.req(url)
            webpage=sp.page.text
            toy_soup2 = soup(webpage, "html.parser")
            gold=toy_soup2.find("div",{"id":"overview-section-4"})
            if len(gold)<1:
                gold=toy_soup2.find("div",{"class":"_2cVsY2"})
            gold = str(gold)
            chambres = re.findall(r"(\d+) chambres", gold)
            if len(chambres)<1:
                chambres = re.findall(r"(\d+) appartements", gold)
            try:
                chambres=chambres[0]
            except:
                chambres=''
            try:
                sp.browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
            except:
                pass
            silver=toy_soup2.find("span",{"class":"star-rating-text star-rating-text-strong widget-star-rating-overlay widget-tooltip widget-tooltip-responsive widget-tooltip-ignore-touch"})
            if len(silver)<1:
                silver=toy_soup2.find("span",{"class":"_2dOcxA"})
            silver2=toy_soup2.find("span",{"class":"star-rating-text widget-star-rating-overlay widget-tooltip widget-tooltip-responsive widget-tooltip-ignore-touch"})
            try:
                silver = str(silver)
                silver2 = str(silver2)
                stars = re.findall(r"(\d?\,?\d) étoiles", silver)
                stars2 = re.findall(r"(\d?\,?\d) étoiles", silver2)
                if len(stars)==0 and len(stars2)>0:
                    stars=stars2[0]
                else:
                    stars=stars[0]
            except:
                stars=''
            bronze = toy_soup2.find("h2")
            if len(bronze)<1:
                bronze = toy_soup2.find("div",{'class':'_2h6Jhd'})
            try:
                bronze=str(bronze)
                bronze=bronze.replace("<h2>","")
                bronze=bronze.replace("</h2>","")
                vname = bronze
            except:
                vname = ""
            try:
                sp.browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
            except:
                pass
            plastic = toy_soup2.find("span",{"class":"postal-addr"})
            if len(plastic)<1:
                plastic = toy_soup2.findAll("span",{"class":"_2wKxGq _1clhIX"})
            try:
                adrs = plastic.text
            except:
                adrs = ""

        except:
            vname=""

        #Check if hotels.com returned valid information

        if flag(x,vname)=='OK':

            varlist=[str(x).replace('\t',''),str(stars).replace('\t',''),str(chambres).replace('\t',''),str(vname).replace('\t',''),str(adrs).replace('\t',''),str(url).replace('\t','')]
            to_append=varlist
            s = pd.DataFrame(to_append).T
            s.to_csv(y, mode='a', header=False,sep='\t',index=False)
            #time.sleep(1)
            pbar.update(1)

        else:

            #Second attempt
            print('checking hrs.com...')
            try:
                url=cosito.hrs
            except:
                url=''

            try:
                sp.req(url)

                description_rating=sp.scrape_light('span',{'class':'product--rating'})
                lectura_rating=description.now()
                sopa_stars=sp.soup(str(lectura_rating),'html.parser')
                stars=sopa_stars.findAll('i',{'class':'icon--star'})
                stars=str(len(stars))

                description_feature=sp.scrape_light('p',{'class':'feature'})
                lectura_feature=description_feature.now()
                regex=re.compile('chambres (\d+)')
                try:
                    chambres=regex.findall(str(lectura_feature[0]))[0]
                except:
                    chambres=""

                description_vname=sp.scrape_light('h1',{'class':'product--title'})
                lecture_vname=description_vname.now()
                try:
                    vname=lecture_vname[0].text.strip()
                except:
                    vname=""

                description_adrs=sp.scrape_light('span',{'class':'location-marker'})
                lecture_adrs=description_adrs.now()
                try:
                    adrs=' '.join(lecture_adrs[0].text.replace('\n','').split())
                except:
                    adrs=""

            except:
                vname=""

            if flag(x,vname)=='OK':

                varlist=[str(x).replace('\t',''),str(stars).replace('\t',''),str(chambres).replace('\t',''),str(vname).replace('\t',''),str(adrs).replace('\t',''),str(url).replace('\t','')]
                to_append=varlist
                s = pd.DataFrame(to_append).T
                s.to_csv(y, mode='a', header=False,sep='\t',index=False)
                #time.sleep(1)
                pbar.update(1)

            else:

                #Third attempt

                print('checking tripadvisor...')

                #Getting url
                try:
                    url=cosito.tripadvisor
                except:
                    url=''
                try:
                    sp.req(url)
                    webpage=sp.page.text
                    trip_soup = soup(webpage, "html.parser")
                    stars_trip=trip_soup.findAll('svg',{'class':'AZd6Ff4E'})
                    capacity_trip=trip_soup.findAll('div',{'id':'ABOUT_TAB'})
                    name_trip=trip_soup.findAll('h1',{'id':'HEADING'})
                    adrs_trip=trip_soup.findAll('span',{'class':'_3ErVArsu jke2_wbp'})

                    try:
                        stars=str(stars_trip[0]['title']).replace(' sur 5\xa0bulles','')
                    except:
                        stars=''
                    try:
                        chambres=str(capacity_trip)
                        roomsy=re.compile('NOMBRE DE CHAMBRES<\/div><div class="_1NHwuRzF">(\d+)')
                        chambres=roomsy.findall(chambres)[0]
                    except:
                        chambres=''
                    try:
                        vname=name_trip[0].text
                    except:
                        vname=''
                    try:
                        adrs=adrs_trip[0].text
                    except:
                        adrs=""
                except:
                    vname=""
                #Check if tripadvisor returned valid information

                if flag(x,vname)=='OK':

                    varlist=[str(x).replace('\t',''),str(stars).replace('\t',''),str(chambres).replace('\t',''),str(vname).replace('\t',''),str(adrs).replace('\t',''),str(url).replace('\t','')]
                    to_append=varlist
                    s = pd.DataFrame(to_append).T
                    s.to_csv(y, mode='a', header=False,sep='\t',index=False)
                    #time.sleep(1)
                    pbar.update(1)

                else:
                    stars=''
                    chambres=''
                    vname=''
                    adrs=''
                    url=''
                    print(x, 'could not be completed','because of',' not found')
                    varlist=[x,stars,chambres,vname,adrs,url]
                    to_append=varlist
                    s = pd.DataFrame(to_append).T
                    s.to_csv(y, mode='a', header=False,sep='\t',index=False)
                    #time.sleep(1)
                    pbar.update(1)

    except Exception as ex:
        stars=''
        chambres=''
        vname=''
        adrs=''
        url=''
        print(x, 'could not be completed','because of',ex)
        varlist=[x,stars,chambres,vname,adrs,url]
        to_append=varlist
        s = pd.DataFrame(to_append).T
        s.to_csv(y, mode='a', header=False,sep='\t',index=False)
        #time.sleep(1)
        pbar.update(1)










# Read lines and put them into a list
def original(y):

    ts = time.gmtime()
    tsx=time.strftime("%s", ts)
    namefile='hotels'+tsx+'.csv'
    fhandle=open(namefile,'w', encoding="utf-8")
    headers = ("Hotel Name"+"\t"+'stars'+'\t'+"Capacities" + '\t' + "webname" + '\t' + "address\n")
    fhandle.write(headers)
    fhandle.close()
    lines = open(y, 'r').readlines()
    pbar=tqdm(total=len(lines))
    sp.open_session_firefox2()

    for line in lines:
        sp.browser.get("https://fr.hotels.com/")
        sp.browser.save_screenshot('test0_original.png')
        try:
            line=line.strip()
            print(line)

            #Manipulate browser in order to obtain individual pages
            time.sleep(0.5)
            try:
                sp.browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
            except:
                pass
            time.sleep(0.5)
            sp.browser.find_element_by_id('qf-0q-destination').clear()
            time.sleep(0.5)
            sp.browser.find_element_by_id('qf-0q-localised-check-in').clear()
            time.sleep(0.5)
            sp.browser.find_element_by_id('qf-0q-localised-check-out').clear()
            time.sleep(0.5)
            sp.browser.find_element_by_id('qf-0q-destination').send_keys(line)
            time.sleep(0.5)
            sp.browser.find_element_by_css_selector('.cont-hd-alt.widget-query-heading').click()
            time.sleep(0.5)
            sp.browser.save_screenshot('test1_original.png')
            counter=0
            while True:
                try:
                    if counter<=10:
                        counter=+1
                        sp.browser.find_element_by_id('qf-0q-destination').send_keys(Keys.ENTER)
                        time.sleep(1)
                    else:
                        break
                except:
                    break
            try:
                sp.browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
                time.sleep(0.5)
            except:
                pass
            time.sleep(0.5)

            # Obtain information on specific site
            sp.browser.save_screenshot('test2_original.png')
            webpage=sp.browser.page_source
            toy_soup2 = soup(webpage, "html.parser")
            gold=toy_soup2.find("div",{"id":"overview-section-4"})
            gold = str(gold)
            chambres = re.findall(r"(\d+) chambres", gold)
            if len(chambres)<1:
                chambres = re.findall(r"(\d+) appartements", gold)
            try:
                chambres=chambres[0]
            except:
                chambres=''
            try:
                sp.browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
            except:
                pass
            silver=toy_soup2.find("span",{"class":"star-rating-text star-rating-text-strong widget-star-rating-overlay widget-tooltip widget-tooltip-responsive widget-tooltip-ignore-touch"})
            silver2=toy_soup2.find("span",{"class":"star-rating-text widget-star-rating-overlay widget-tooltip widget-tooltip-responsive widget-tooltip-ignore-touch"})
            try:
                silver = str(silver)
                silver2 = str(silver2)
                stars = re.findall(r"(\d?\,?\d) étoiles", silver)
                stars2 = re.findall(r"(\d?\,?\d) étoiles", silver2)
                if len(stars)==0 and len(stars2)>0:
                    stars=stars2[0]
                else:
                    stars=stars[0]
            except:
                stars=''
            bronze = toy_soup2.find("h2")
            try:
                bronze=str(bronze)
                bronze=bronze.replace("<h2>","")
                bronze=bronze.replace("</h2>","")
                vname = bronze
            except:
                vname = ""
            try:
                sp.browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
            except:
                pass
            plastic = toy_soup2.find("span",{"class":"postal-addr"})
            try:
                adrs = plastic.text
            except:
                adrs = ""

            varlist=[line,nom,type,rank,location,comments,note,phone]
            to_append=varlist
            s = pd.DataFrame(to_append).T
            s.to_csv(namefile, mode='a', header=False,sep='\t',index=False)
            #time.sleep(1)
            pbar.update(1)
        except Exception as ex:
            stars=''
            chambres=''
            vname=''
            adrs=''
            print(line, 'could not be completed','because of',ex)
            varlist=[line,stars,chambres,vname,adrs]
            to_append=varlist
            s = pd.DataFrame(to_append).T
            s.to_csv(y, mode='a', header=False,sep='\t',index=False)
            #time.sleep(1)
            pbar.update(1)



def alternative(x,y):
    try:
        sp.browser.get("https://fr.hotels.com/")
        #sp.browser.save_screenshot('test0.png')
        x=x.strip()
        print(x)

        #Manipulate sp.browser in order to obtain individual pages
        time.sleep(0.5)
        try:
            sp.browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
        except:
            pass

        #time.sleep(0.3)
        #sp.browser.find_element_by_xpath('//*[@id="qf-1q-localised-check-in"]').clear()
        #time.sleep(0.3)
        #sp.browser.find_element_by_xpath('//*[@id="qf-1q-localised-check-out"]').clear()
        #time.sleep(0.3)
        #sp.browser.find_element_by_xpath('//*[@id="qf-1q-destination"]').click()
        #time.sleep(0.3)
        #sp.browser.find_element_by_xpath('//*[@id="qf-1q-destination"]').clear()
        sp.browser.find_element_by_xpath('/html/body/div[2]/div/div/div/div/header/div/form/fieldset/div').clear()
        time.sleep(0.3)
        sp.browser.find_element_by_xpath('/html/body/div[2]/div/div/div/div/header/div/form/fieldset/div/input').send_keys(x)
        time.sleep(0.3)
        sp.browser.find_element_by_xpath('/html/body/div/main/div[1]/div[2]/div/form/div[2]/div[3]/button').click()
        time.sleep(0.8)
        try:
            sp.browser.find_element_by_xpath('/html/body/div/main/div[1]/div[2]/div/form/div[2]/div[3]/button').click()
        except:
            pass
        '''
        tries=0
        while True:
            try:
                sp.browser.find_element_by_xpath('/html/body/div/main/div[1]/div[2]/div/form/div[2]/div[3]/button').click()
                time.sleep(15)
                #sp.browser.save_screenshot('testtemp.png')
            except:
                break
        '''

        '''
        sp.browser.find_element_by_xpath('//*[@id="qf-1q-destination"]').send_keys(Keys.ENTER)
        #sp.browser.save_screenshot('test.png')while True:
        time.sleep(0.3)
        try:
            sp.browser.find_element_by_xpath('//*[@id="qf-1q-destination"]').send_keys(Keys.ENTER)
            myElem = WebDriverWait(sp.browser, 5).until(EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/main/div[4]/div[1]/div[1]/div/div[1]/h1')))

        except:
            button_=sp.browser.find_element_by_css_selector('.widget-query-geo > form:nth-child(2) > div:nth-child(4) > button:nth-child(1)')
            sp.browser.execute_script("arguments[0].scrollIntoView();", button_)
            button_.click()
            while True:
                try:
                    time.sleep(2)
                    sp.browser.save_screenshot('testtemp.png')
                    myElem = WebDriverWait(sp.browser, 5).until(EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/main/div[4]/div[1]/div[1]/div/div[1]/h1')))
                except:
                    button_.click()
                    time.sleep(2)
        #sp.browser.execute_script("arguments[0].scrollIntoView();", button_)
        #button_.click()
        #sp.browser.find_element_by_xpath('//*[@id="qf-1q-destination"]').send_keys(Keys.ENTER)
        #time.sleep(3)
        sp.browser.save_screenshot('test2.png')
        #time.sleep(1)
        '''
        '''
        countertry=0
        while countertry<=2:
            try:
                sp.browser.find_element_by_xpath('//*[@id="qf-1q-destination"]').send_keys(Keys.ENTER)
                time.sleep(1.5)
                countertry=+1
            except:
                break
        '''
        try:
            sp.browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
            time.sleep(0.5)
        except:
            pass
        #time.sleep(2)
        #sp.browser.save_screenshot('test3.png')
        # Obtain information on specific site
        #sp.browser.save_screenshot('test3.png')
        webpage=sp.browser.page_source
        toy_soup2 = soup(webpage, "html.parser")
        gold=toy_soup2.find("div",{"class":"_2cVsY2"})
        gold = str(gold)
        chambres = re.findall(r"(\d+) chambres", gold)
        if len(chambres)<1:
            chambres = re.findall(r"(\d+) appartements", gold)
        try:
            chambres=chambres[0]
        except:
            chambres=''
        try:
            sp.browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
        except:
            pass
        silver=toy_soup2.find("span",{"class":"_2dOcxA"})
        silver2=toy_soup2.find("span",{"class":"star-rating-text widget-star-rating-overlay widget-tooltip widget-tooltip-responsive widget-tooltip-ignore-touch"})
        try:
            silver = str(silver)
            silver2 = str(silver2)
            stars = re.findall(r"(\d?\,?\d) étoiles", silver)
            stars2 = re.findall(r"(\d?\,?\d) étoiles", silver2)
            if len(stars)==0 and len(stars2)>0:
                stars=stars2[0]
            else:
                stars=stars[0]
        except:
            stars=''
        bronze = toy_soup2.find("div",{'class':'_2h6Jhd'})
        try:
            bronze=str(bronze.h1.text)
            #bronze=bronze.replace("<h2>","")
            #bronze=bronze.replace("</h2>","")
            vname = bronze
        except:
            vname = ""
        try:
            sp.browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
        except:
            pass
        plastic = toy_soup2.findAll("span",{"class":"_2wKxGq _1clhIX"})
        try:
            adrs = plastic[2].text.replace('\ue98d',"")
        except:
            adrs = ""

        varlist=[x,stars,chambres,vname,adrs]
        to_append=varlist
        s = pd.DataFrame(to_append).T
        s.to_csv(y, mode='a', header=False,sep='\t',index=False)
        #time.sleep(1)
        #pbar.update(1)
    except Exception as ex:
        stars=''
        chambres=''
        vname=''
        adrs=''
        print(x, 'could not be completed','because of',ex)
        varlist=[x,stars,chambres,vname,adrs]
        to_append=varlist
        s = pd.DataFrame(to_append).T
        s.to_csv(y, mode='a', header=False,sep='\t',index=False)
        #time.sleep(1)
        #pbar.update(1)

# Define function hotel

def pointer(x):
    ts = time.gmtime()
    tsx=time.strftime("%s", ts)
    namefile='hotels'+tsx+'.csv'
    fhandle=open(namefile,'w', encoding="utf-8")
    headers = ("Hotel Name"+"\t"+'stars'+'\t'+"Capacities" + '\t' + "webname" + '\t' + "address\n")
    fhandle.write(headers)
    fhandle.close()
    lines = open(x, 'r').readlines()
    pbar=tqdm(total=len(lines))
    sp.open_session_firefox2()
    sp.browser.get("https://fr.hotels.com/")
    sp.browser.save_screenshot('test0.png')
    #Manipulate sp.browser in order to obtain individual pages
    time.sleep(0.5)
    try:
        sp.browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
    except:
        pass
    time.sleep(0.5)
    try:
        sp.browser.find_element_by_id('qf-0q-destination').clear()
        for line in lines:
            original(line,namefile)
            pbar.update(1)
        sp.browser.quit()
    except:
        for line in lines:
            alternative(line,namefile)
            pbar.update(1)
        sp.browser.quit()
