import os
import re
from time import sleep
from datetime import datetime
import pandas as pd
from numpy import double
from sqlalchemy import exc
from selenium.webdriver.support.select import Select
from selenium_helpers import find_if_exists_by_xpath, initialize
from sqlalchemy import create_engine

env = {}
def load_env():
	with open("/home/oem/Documents/PROJECTS/orange_money/.env") as f:
		for line in f:
			if line.startswith("#") or line.isspace():
				continue
			key, value = line.strip().split('=', 1)
			env[key] = value

load_env()

def db_str():
    username = env['DB_USERNAME']
    password = env['DB_PASSWORD'].strip("'").strip('"')
    password = re.sub('@', '%40', password)
    host = env['DB_HOST']
    db_name = env['DB_DATABASE']

    return "{0}:{1}@{2}/{3}".format(username, password, host, db_name)
    
def parse_date(old_date):

    if('/' in old_date):
        return datetime.strptime(old_date, "%d/%m/%y").strftime("%Y-%m-%d")
    else:
        return Exception("")

stmt_ngin = create_engine('mysql+mysqlconnector://' + db_str(), pool_size=10)

def rename(txn_df):
    txn_df = txn_df.drop(columns=['Value date', 'Ccy'])
    txn_df.rename(columns={
                            'Op. Date': 'stmt_txn_date',
                            'Account number': 'acc_number',
                            'Description': 'descr',
                            'Reference': 'stmt_txn_id',
                            'Debit': 'dr_amt',
                            'Credit': 'cr_amt',
                            'Running balance': 'balance' 
                            },inplace=True)
    
    return txn_df


def load_df(txn_df , is_row_df = False, row_index = 0):

    #Iterate one row at a time
    for i in range(len(txn_df)):
        try:
            iterate_by_row = txn_df.iloc[i:i+1]
            iterate_by_row.to_sql('account_stmts', con = stmt_ngin, if_exists='append', 
            chunksize = 500, index = False)
        except exc.IntegrityError as e:
            err = e.orig.args
            if('Duplicate entry' in err[1]):
                print("!!! Duplicate entry "+str(i)+"th Row !!!")
                pass
            else:
                raise (err)

def is_num(value):

	if value == '':
		return False
	try:
		import math
		return not math.isnan(float(value))
	except:
		return True

def to_num(amt):

    if(type(amt) is str):
        import re
        # amt =  re.sub('[^-.0-9]+', '', (amt.strip('UGX')))
        amt =  re.sub('[^-.0-9]+', '', amt)
    if is_num(amt):
        return abs(double(amt))
    else:
        return 0.00

def stmt_type(row):

    if(row['cr_amt'] == 0):
        return 'debit'
    elif(row['dr_amt'] == 0):
        return 'credit'

def to_ref_account(row):

    if row.find("BOA to ORANGE") != -1:
        return row[20: 30]
    else:
        return ""

def file_download_info(download_path, old_file_count):

    max_time = 20
    new_file_count = 0
    count = 0
    for i in range(max_time):
        sleep(1)
        new_files = os.listdir(download_path)
        new_file_count = len(new_files)
        if new_file_count > old_file_count:
            return("File downloaded")
        elif count == max_time:
            return("Time Out")
        else:
            pass

def get_amount(row):

    if(row['cr_amt'] == 0):
        return row['dr_amt']
    elif(row['dr_amt'] == 0):
        return row['cr_amt']

def to_group(row): 

    if row.find("BOA to ORANGE") != -1:
        return "boa_tdr"
    else:
        return ""

def concat_tm(row):
    hr = "00:00:00"
    row = row +" "+ hr
    return row
    

def db_transform(df):

    data = rename(df)
    data['stmt_txn_date'] = data['stmt_txn_date'].apply(parse_date)
    data['stmt_txn_date'] = data['stmt_txn_date'].apply(concat_tm)
    data['acc_prvdr_code'] = "BOA"
    data['country_code'] = "MAG"
    data['group'] = data['descr'].apply(to_group)
    data['ref_account_num'] = data['descr'].apply(to_ref_account)
    data['dr_amt'] = data['dr_amt'].fillna(0)
    data['dr_amt'] = data['dr_amt'].apply(to_num)
    data['cr_amt'] = data['cr_amt'].fillna(0) 
    data['cr_amt'] = data['cr_amt'].apply(to_num)
    data['stmt_txn_type'] = data.apply(stmt_type, axis=1)
    data['amount'] = data.apply(get_amount, axis=1)
    data = data.drop(['cr_amt'], axis=1)
    data = data.drop(['dr_amt'], axis=1)
    
    print(data)
    load = load_df(data)
    return load

try:
    global driver
    driver = initialize()
    driver.get('https://boaweb.of.africa/')
    find_if_exists_by_xpath("//div/div[contains(@class, 'corpo-block')]/center/a").click()
    find_if_exists_by_xpath("//input[contains(@id, 'user_login')]").send_keys('MG000829501')
    driver.execute_script("document.getElementById('user_password').value='290388'")
    find_if_exists_by_xpath("//input[contains(@id, 'login-submit-button')]").click()
    find_if_exists_by_xpath("//a[contains(@id, 'navbarDropdownMenuLink')]").click()
    find_if_exists_by_xpath("//div[contains(@id, 'menuNav')]/ul/li[2]/div/a[4]").click()
   
    account_list = [
        "00009021002042323000518",
        "00009024002042323001838",
    ]
    for index, account in enumerate(account_list):
        full_acc_no = "FLOW MADAGASCAR - "+account
        sel = Select(find_if_exists_by_xpath("//select[contains(@id, 'account_id')]"))
        sel.select_by_visible_text(full_acc_no)
        if index == 0:
            find_if_exists_by_xpath("//input[contains(@id, 'start_date')]").send_keys('2022-08-20')   
            find_if_exists_by_xpath("//input[contains(@id, 'end_date')]").send_keys('2022-08-31')
        find_if_exists_by_xpath("//button[contains(@class, 'btn btn-primary')]").click()

        download_path = "/home/oem/Downloads"
        lst_files = os.listdir(download_path)
        old_file_count = len(lst_files)
        
        find_if_exists_by_xpath("//div[contains(@class, 'card-header')]/div[contains(@class, 'text-lg-right text-center')]/a[2]").click()
        file_dw = file_download_info(download_path, old_file_count)
        print(file_dw)
        filename = max([download_path + "/" + f for f in os.listdir(download_path)],key = os.path.getctime)
        df = pd.read_excel(filename)
        db_transform(df) 
        

 
except Exception as e:
    print('exception')
    print(e)
finally:
#   sleep(20)
    driver.close()