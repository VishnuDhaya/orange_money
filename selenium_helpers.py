from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, TimeoutException 
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import date
import sys, time, os



WAIT = 12

def wait_for_visibility(by, token, wait_sec = 8):
	wait = WebDriverWait(driver, wait_sec)
	wait.until(EC.visibility_of_element_located((by, token)))


def save_screenshot(base_path, sub_path, event, data = None):
    
    if(driver):
        milliseconds = int(round(time.time() * 1000))   
        final_path = ''
        date_str = date.today().strftime("%Y-%m-%d")
      
        final_path = base_path + '/' + sub_path + '/' + date_str + '/' + event
        if not os.path.exists(final_path):
            os.makedirs(final_path)

        if(data):        
            path = final_path + "/" + data + "_" + str(milliseconds) + ".png"
            
        driver.save_screenshot(final_path + "/" + data + "_" + str(milliseconds) + ".png")


def find_if_exists_by_xpath(xpath, wait_secs=8):
    try:
        wait_for_visibility(By.XPATH, xpath, wait_secs)
        return driver.find_element_by_xpath(xpath)
    except (NoSuchElementException, TimeoutException) as err:
        return None

def find_if_exists_by_link_text(link_text):
    try:
        wait_for_visibility(By.LINK_TEXT, link_text)
        return driver.find_element_by_link_text(link_text)
    except (NoSuchElementException, TimeoutException) as err:
        return None


def initialize():
    global driver
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
 
    # driver = webdriver.Chrome('/usr/lib/python2.7/site-packages/chromedriver_binary/chromedriver', options = chrome_options)
    driver = webdriver.Chrome(ChromeDriverManager().install())
    driver.set_window_size(1920, 1080)
    driver.maximize_window() 
    driver.implicitly_wait(WAIT) # seconds
    return driver
