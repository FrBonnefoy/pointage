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

# Read lines and put them into a list

#lines = open('items.txt', 'r').readlines()

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
    for line in lines:
        open_session_firefox()
        browser.get("https://fr.hotels.com/")
        browser.save_screenshot('test0.png')
        line=line.strip()
        print(line)

        #Manipulate browser in order to obtain individual pages
        time.sleep(0.5)
        try:
            browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
        except:
            pass
        time.sleep(1)
        browser.find_element_by_id('qf-0q-destination').clear()
        time.sleep(1)
        browser.find_element_by_id('qf-0q-localised-check-in').clear()
        time.sleep(1)
        browser.find_element_by_id('qf-0q-localised-check-out').clear()
        time.sleep(1)
        browser.find_element_by_id('qf-0q-destination').send_keys(line)
        time.sleep(1)
        browser.save_screenshot('test.png')
        browser.find_element_by_id('qf-0q-destination').send_keys(Keys.ENTER)
        time.sleep(3)
        browser.save_screenshot('test2.png')
        time.sleep(1)
        while True:
            try:
                browser.find_element_by_id('qf-0q-destination').send_keys(Keys.ENTER)
                time.sleep(1)
            except:
                break
        try:
            browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
            time.sleep(1)
        except:
            pass
        time.sleep(2)
        browser.save_screenshot('test3.png')
        # Obtain information on specific site
        webpage=browser.page_source
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
            browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
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
            browser.find_element_by_css_selector('.cta.widget-overlay-close').click()
        except:
            pass
        plastic = toy_soup2.find("span",{"class":"postal-addr"})
        try:
            adrs = plastic.text
        except:
            adrs = ""

        varlist=[line,stars,chambres,vname,adrs]
        to_append=varlist
        s = pd.DataFrame(to_append).T
        s.to_csv(namefile, mode='a', header=False,sep='\t',index=False)
        time.sleep(1)
        pbar.update(1)
    close_session()

