from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup as soup
import time
import requests
import warnings
warnings.filterwarnings('ignore', message='Unverified HTTPS request')
from IPython.display import Image
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import glob
import csv
from xlsxwriter.workbook import Workbook
import pandas as pd
from IPython.display import display
import re
current_path=os.getcwd()


#Define proxies

http_proxy  = "http://127.0.0.1:24000"
https_proxy = "https://127.0.0.1:24000"
ftp_proxy   = "ftp://127.0.0.1:24000"

proxyDict = {
              "http"  : http_proxy,
              "https" : https_proxy,
              "ftp"   : ftp_proxy
            }

#Define requests function
def req(x):
    global page
    page=requests.get(x,proxies=proxyDict,verify=False)

#Define requests function
def req2(x):
    global page
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    page=requests.get(x,proxies=proxyDict,verify=False,headers=headers)

http_proxy2  = "http://127.0.0.1:24003"
https_proxy2 = "https://127.0.0.1:24003"
ftp_proxy2   = "ftp://127.0.0.1:24003"

proxyDict2 = {
              "http2"  : http_proxy,
              "https2" : https_proxy,
              "ftp2"   : ftp_proxy
            }


def req_google(x):
    global page
    global google_url

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    page=requests.get(x,proxies=proxyDict,verify=False,headers=headers)
    description=scrape_light('div',{'class':'yuRUbf'})
    lecture=description.now()
    try:
        google_url=lecture[0].a['href']
    except:
        google_url=""


def help():

    print( '''
open_session() : Ouvre une nouvelle séance de chrome avec une nouvelle adresse IP

close_session() : Fermer la session avant de reouvrir une nouvelle avec une nouvelle adresse IP (économise des ressources)

change(x) : Aller sur le site x avec une séance ouvert de chrome. Ex: change('https://www.google.com/') pour aller à Google.

data(): Lire le code html tel comme il est présenté dans le navigateur virtuel.

scroll(): Aller à la fin de la page.

screnshoot(x): Sauvegarder une capture d'écran du navigateur avec l'image x. Ex: screenshot('test.png')

screen(): Sauvegarder une capture d'écran du navigateur sous le nom 'browser.png'

scrape(x,y): Trouver tous les éléments avec les identifiants html x et y. Ex: identifiant= h2 class="mb0" -> x='h2' ; y={'class':'mb0'}. Le data scrape s'effectue quand la fonction .now() est appellée.

printext(x): Imprimer le texte trouvé sur la console

geturls(x): Capturer les urls dans le récipient html specifié par scrape dans une liste appellée urls.

printhtml(x): Imprimer le code html de tous les récipients qui s'allignent avec la définition donnée par scrape.
            ''')


def image(x):
    Image(filename=x)

def open_session_firefox_no_proxy():
    global browser
    options = FirefoxOptions()
    options.add_argument("--headless")
    options.add_argument("--window-size=1280×720")
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
    #browser.install_addon(current_path+"/disable_webrtc-1.0.23-an+fx.xpi", temporary=True)
    #browser.install_addon(current_path+"/image_block-5.0-fx.xpi", temporary=True)
    #browser.install_addon(current_path+"/ublock_origin-1.31.0-an+fx.xpi", temporary=True)

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
    options.add_argument("--window-size=1024x5000")
    #options.add_argument('start-maximized')
    profile = webdriver.FirefoxProfile()
    #profile.add_extension(current_path+"/disable_webrtc-1.0.23-an+fx.xpi")
    #profile.add_extension(current_path+"/adblock_for_firefox-4.24.1-fx.xpi")
    #profile.add_extension(current_path+"/image_block-5.0-fx.xpi")
    #profile.add_extension(current_path+"/ublock_origin-1.31.0-an+fx.xpi")
    profile.DEFAULT_PREFERENCES['frozen']["media.peerconnection.enabled" ] = False
    profile.set_preference("media.peerconnection.enabled", False)
    #profile.set_preference("permissions.default.image", 2)
    profile.update_preferences()
    browser = webdriver.Firefox(profile,options=options)
    #browser.install_addon(current_path+"/disable_webrtc-1.0.23-an+fx.xpi", temporary=True)
    #browser.install_addon(current_path+"/image_block-5.0-fx.xpi", temporary=True)
    #browser.install_addon(current_path+"/ublock_origin-1.31.0-an+fx.xpi", temporary=True)

def open_session_firefox2():
    global browser
    PROXY="127.0.0.1:24002"
    webdriver.DesiredCapabilities.FIREFOX['proxy'] = {
    "httpProxy": PROXY,
    "ftpProxy": PROXY,
    "sslProxy": PROXY,
    "proxyType": "MANUAL",
    }
    options = FirefoxOptions()
    options.add_argument('--proxy-server=%s' % PROXY)
    options.add_argument("--headless")
    options.add_argument("--window-size=1024x5000")
    options.add_argument("--private")
    #options.add_argument("user-agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36")
    #options.add_argument('start-maximized')
    profile = webdriver.FirefoxProfile()
    #profile.add_extension(current_path+"/disable_webrtc-1.0.23-an+fx.xpi")
    #profile.add_extension(current_path+"/adblock_for_firefox-4.24.1-fx.xpi")
    #profile.add_extension(current_path+"/image_block-5.0-fx.xpi")
    #profile.add_extension(current_path+"/ublock_origin-1.31.0-an+fx.xpi")
    profile.DEFAULT_PREFERENCES['frozen']["media.peerconnection.enabled" ] = False
    profile.set_preference("media.peerconnection.enabled", False)
    profile.set_preference("browser.privatebrowsing.autostart", True)
    #profile.set_preference("general.useragent.override", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36")
    #profile.set_preference("permissions.default.image", 2)
    profile.update_preferences()
    browser = webdriver.Firefox(profile,options=options)
    #browser.install_addon(current_path+"/disable_webrtc-1.0.23-an+fx.xpi", temporary=True)
    #browser.install_addon(current_path+"/image_block-5.0-fx.xpi", temporary=True)
    #browser.install_addon(current_path+"/ublock_origin-1.31.0-an+fx.xpi", temporary=True)


def open_session():
    global browser
    PROXY = "127.0.0.1:24001"
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--proxy-server=%s' % PROXY)
    chrome_options.add_argument("--window-size=1920x4080")
    chrome_options.add_argument('start-maximized')
    chrome_options.add_argument('disable-infobars')
    chrome_options.add_extension('~/webrtc.crx')
    '''
    preferences = {
    "webrtc.ip_handling_policy" : "disable_non_proxied_udp",
    "webrtc.multiple_routes_enabled": False,
    "webrtc.nonproxied_udp_enabled" : False,
    'profile.managed_default_content_settings.javascript': 2,
    "enforce-webrtc-ip-permission-check": True
    }
    chrome_options.add_experimental_option("prefs", preferences)
    chrome_options.add_argument('--force-webrtc-ip-handling-policy')
    '''
    browser = webdriver.Chrome(options=chrome_options)


def open_session2():
    global browser
    PROXY = "127.0.0.1:24002"
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--proxy-server=%s' % PROXY)
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument('start-maximized')
    chrome_options.add_argument('disable-infobars')
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
    chrome_options.add_argument('user-agent={0}'.format(user_agent))
    #chrome_options.add_extension('~/webrtc.crx')
    '''
    preferences = {
    "webrtc.ip_handling_policy" : "disable_non_proxied_udp",
    "webrtc.multiple_routes_enabled": False,
    "webrtc.nonproxied_udp_enabled" : False,
    'profile.managed_default_content_settings.javascript': 2,
    "enforce-webrtc-ip-permission-check": True
    }
    chrome_options.add_experimental_option("prefs", preferences)
    chrome_options.add_argument('--force-webrtc-ip-handling-policy')
    '''
    browser = webdriver.Chrome(options=chrome_options)

def screenshot(x):
    browser.save_screenshot(x)

def screen():
	browser.save_screenshot('browser.png')

def close_session():
    browser.close()

def data():
	global content
	content=browser.page_source
	global sopa
	sopa=soup(content,'html.parser')

def scroll():
	height=0
	height2=1
	while height!=height2:
		height= browser.execute_script("return $(document).height()")
		browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
		time.sleep(2)
		height2 = browser.execute_script("return $(document).height()")
	browser.save_screenshot("endscroll.png")

def change(x):
	browser.get(x)



class scrape:
    def __init__(self,x,y=None):
        self.x = x
        self.y = y if y is not None else x
    def now(self):
        content=browser.page_source
        sopa=soup(content,'html.parser')
        if self.x==self.y:
            return sopa.findAll(self.x)
        else:
            return sopa.findAll(self.x,self.y)
    def find(self,z):
        treasure=re.compile(z)
        tempfind=[]
        content=browser.page_source
        sopa=soup(content,'html.parser')
        if self.x==self.y:
            nugget=sopa.findAll(self.x)
        else:
            nugget=sopa.findAll(self.x,self.y)
        for a in nugget:
            findings=treasure.findall(a.text)
            tempfind.append(findings)
        return tempfind

class scrape_light:
    def __init__(self,x,y=None):
        self.x = x
        self.y = y if y is not None else x
    def now(self):
        content=page.text
        sopa=soup(content,'html.parser')
        if self.x==self.y:
            return sopa.findAll(self.x)
        else:
            return sopa.findAll(self.x,self.y)
    def find(self,z):
        treasure=re.compile(z)
        tempfind=[]
        content=page.text
        sopa=soup(content,'html.parser')
        if self.x==self.y:
            nugget=sopa.findAll(self.x)
        else:
            nugget=sopa.findAll(self.x,self.y)
        for a in nugget:
            tempfind=treasure.findall(a.text)
            if len(tempfind)==0:
                tempfind.append("")
            #tempfind.append(findings)
        return tempfind

def printext(x):
    for a in x:
        print(a.text.strip())


def geturls(x):
    global urls
    urls=[]
    for a in x:
        try:
            urls.append(a['href'])
        except:
            pass

def alterurls(x,y):
    return list(map(lambda z: y+z,x))

def printhtml(x):
    for a in x:
        print(a)

def excelfy():
    for csvfile in glob.glob(os.path.join('.', '*.csv')):
        df=pd.read_csv(csvfile, sep='\t')
        excelfile=csvfile[:-4] + '.xlsx'
        df.to_excel(excelfile, index = False)
        display(df)

def excelfy_specific(x):
    df=pd.read_csv(x,sep='\t')
    excelfile=x[:-4] + '.xlsx'
    df.to_excel(excelfile, index = False)
    display(df)

def reste_a_pointer(x,y,z):
    if x[-4:]=='.csv':
        df=pd.read_csv(x, sep='\t')
        filtered_df = df[df[z].isnull()]
        filtered_df=filtered_df[~filtered_df[y].isnull()]
        noms = filtered_df[y].tolist()
        with open(x[:-4]+'_a_pointer.txt','w') as f:
            for nom in noms:
                print(str(nom).strip(),file=f)
                print(nom)
    if x[-5:]=='.xlsx':
        df=pd.read_excel(x)
        filtered_df = df[df[z].isnull()]
        filtered_df=filtered_df[~filtered_df[y].isnull()]
        noms = filtered_df[y].tolist()
        with open(x[:-4]+'_a_pointer.txt','w') as f:
            for nom in noms:
                print(str(nom).strip(),file=f)
                print(nom)
