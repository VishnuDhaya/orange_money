from multiprocessing.context import SpawnContext
import re
import math
import os
import pandas as pd
from time import sleep
from numpy import double
from sqlalchemy import exc
from datetime import datetime
from sqlalchemy import create_engine
from selenium_helpers import find_if_exists_by_xpath, initialize

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

stmt_ngin = create_engine('mysql+mysqlconnector://' + db_str(), pool_size=10)

def concat_dt_tm(row):

    date = row['stmt_txn_date']
    hr = row['hour']
    row['stmt_txn_date'] = date +" "+ hr
    return row['stmt_txn_date']

# def get_type(row):

#     if(row['cr_amt'] == 0):
#         return 'debit'
#     elif(row['dr_amt'] == 0):
#         return 'credit'

# def get_amount(row):
    
#     if(row['cr_amt'] == 0):
#         return row['dr_amt']
#     elif(row['dr_amt'] == 0):
#         return row['cr_amt']

# def get_balance(row, run_bal):

#     if(row['cr_amt'] == 0):
#         bal = row['amount'] - run_bal
#         return bal
#     elif(row['dr_amt'] == 0):
#         bal = row['amount'] + run_bal
#         return bal

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
        # amt =  re.sub('[^-.0-9]+', '', (amt.strip('UGX')))
        amt =  re.sub('[^-.0-9]+', '', amt)
    if is_num(amt):
        return abs(double(amt))
    else:
        return 0.00

def parse_date(old_date):

    if('/' in old_date):
        return datetime.strptime(old_date, "%d/%m/%Y").strftime("%Y-%m-%d")
    else:
        return Exception("")

def ed_pt(data):

    for x in data.itertuples():
        solde_check = x._12 == "Solde final"
        if solde_check == True:
            end_point = x.Index
            break
    return end_point

def int_bal_ck(start_point):

    bal = df.iloc[[start_point + 5]]
    initial_bal_check = bal.iloc[0:, 14].item()
    return initial_bal_check

def txn_date(start_point):
    dt = df.iloc[start_point - 5, 3]
    dt = parse_date(dt)
    return dt

def finl_bal_ck(end_point):

    f_bal = df.iloc[[end_point]]
    final_bal_check = f_bal.iloc[0:, 14].item()
    return final_bal_check

def nw_df_cl(start_point):

    table_title = df.iloc[[start_point + 4]]
    header_row = table_title.iloc[[0]] 
    header_row['Unnamed: 11'].replace('N° de Compte', 'ref_account_num', inplace=True)
    return header_row

def nw_df_rw(fltr_indx, spcfic_data):
    
    new_data = pd.DataFrame()
    for i in fltr_indx:
        new_data = new_data.append(spcfic_data.iloc[i])
    return new_data

def find_accno(rf):
    boa_accno = [
        '0325186587'
    ]
    tdr_accno = [
        '0322285302'
    ]
    prt_accno = [
        '0322284219'
    ]
    agent_accno = [
        '0323847965',
        '0321550231',
        '0322285302',
        '0324266121',
        '0322646571',
        '0324812678',
        '0327131771',
        '0328044684',
        '0322646537',
        '0327134631',
        '0327134320',
        '0324266121',
    ]
    for x in boa_accno:
        if x == rf:
            return "boa"
    
    for x in tdr_accno:
        if x == rf:
            return "tdr"

    for x in prt_accno:
        if x == rf:
            return "prt"
    
    for x in agent_accno:
        if x == rf:
            return "agent"

def restr_txn(dtf, start_point):
    txn = []
    for x in dtf.itertuples():
        tx = getattr(x, 'Référence')
        acc_no = getattr(x, 'ref_account_num')
        if pd.isnull(tx):
            index_no = x._1
            dt = txn_date(start_point)
            trsn = str(index_no)+"-"+str(dt)+"-"+str(acc_no)
            txn.append(trsn)
        else:
            txn.append(tx)
    return txn

def filter_data(spcfic_data):

    fltr_indx = []
    for x in spcfic_data.itertuples():
        # print(x)
        # print(getattr(x, 'Index'))
        status_check = x._7 == "Succès"
        com_check = x._5 == "Commissions venant de"
        if status_check == True or com_check == True:
            index = getattr(x, 'Index')
            fltr_indx.append(index)
    return fltr_indx

def run_bal_grp(dtf, initial_bal, final_bal):
    bal = []
    grp = []
    amt = []
    tx_type = []

    for x in dtf.itertuples():
        de = getattr(x, 'Débit')
        cr = getattr(x, 'Crédit')
        comisn = x._16
        if math.isnan(de) and math.isnan(cr):
            bl_amt = initial_bal + comisn
            bal.append(bl_amt)
            grp.append("commission")
            amt.append(comisn) 
            tx_type.append("credit")
        elif math.isnan(de):
            st = getattr(x, 'Statut')
            if st =='Succès':
                rf = getattr(x, 'ref_account_num')
                ckacc_no = find_accno(rf)
                myacc_no = find_accno(x._9)
                grp_name = str(ckacc_no)+"_"+str(myacc_no)
                if grp_name == "agent_prt":
                    grp_name = "buy_float"
                
                grp.append(grp_name)
            bl_amt = initial_bal + cr
            bal.append(bl_amt)
            amt.append(cr)
            tx_type.append("credit")
        elif math.isnan(cr):
            st = getattr(x, 'Statut')
            if st =='Succès':
                rf = getattr(x, 'ref_account_num')
                ckacc_no = find_accno(rf)
                myacc_no = find_accno(x._9)
                grp_name = str(myacc_no)+"_"+str(ckacc_no)
                if grp_name == "prt_agent":
                    grp_name = "sell_float"
                grp.append(grp_name)
            bl_amt = initial_bal - de
            bal.append(bl_amt)
            amt.append(de)
            tx_type.append("debit")
        initial_bal = bl_amt
        print(x)
    print(grp)
    last_run_bal = bal[-1]
    if last_run_bal == final_bal:
        print("!!! Balance amount success !!!")
    else:
        print("??? Failed Balance generation ???")

    return bal, grp, amt, tx_type

def dt_hr(dtf, start_point):
    dates = []
    hours = []
    for x in dtf.itertuples():
        date = getattr(x, 'Date')
        hour = getattr(x, 'Heure')
        hr = "00:00:00"
        if pd.isnull(date) and pd.isnull(hour):
            dt = txn_date(start_point)
            hours.append(hr)
            dates.append(dt)      
        else:
            dt = parse_date(date)
            dates.append(dt)
            hours.append(hour)
    return dates, hours

def load_df(txn_df , is_row_df = False, row_index = 0):

    #Iterate one row at a time
    for i in range(len(txn_df)):
        try:
            iterate_by_row = txn_df.iloc[i:i+1]
            iterate_by_row.to_sql('account_stmts', con = stmt_ngin, if_exists='append', 
            chunksize = 500, index = False)
            print(str(i)+"th Record Inserted Succesfully!!!")
        except exc.IntegrityError as e:
            err = e.orig.args
            if('Duplicate entry' in err[1]):
                print("??? Duplicate entry "+str(i)+"th Record ???")
                pass
            else:
                raise (err)

def rename(txn_df):

    txn_df = txn_df.drop(columns=["N°", "Service", "Paiement", 
                        "Statut", "Mode", "Wallet",
                        "N° Pseudo", "Wallet", "Débit", "Crédit",
                        "Super-distributeur", "Sous-distributeur"], axis = 1)
    txn_df.rename(columns={
                            'Date': 'stmt_txn_date',
                            'Heure': 'hour',
                            'N° de Compte': 'acc_number',
                            'Référence': 'stmt_txn_id'
                            },inplace=True)
    print(txn_df.iloc[[0]])
    print('dhaya----001')
    return txn_df   

def db_transform(df):
   
    data = rename(df)
    print(data)
    data['acc_prvdr_code'] = "ORA"
    data['country_code'] = "MAG"
    data['descr'] = ""
    data['stmt_txn_date'] = data.apply(concat_dt_tm, axis = 1)    
    data = data.drop(['hour'], axis=1)
    print("dhaya-----02")
    # print(data.iloc[:, 4:])
    load  = load_df(data)
    print(load)
   

def table_data(start_point):

    # ed_loc = data.loc[data["N° de Compte"].str.contains("Solde final", case=False, na=False)]
    data = df.iloc[start_point:,]
    end_point = ed_pt(data)
    
    # Filtering Transction table data
    spcfic_data = df.iloc[start_point + 4: end_point + 1,]
    spcfic_data.reset_index(drop=True, inplace=True)

    # Filtering succcessful transaction data
    fltr_indx = filter_data(spcfic_data)

    # Spilt dataframe rows
    new_df_row = nw_df_rw(fltr_indx, spcfic_data)

    # Spilt dataframe head columns
    new_df_col = nw_df_cl(start_point)
   
    # Merge new dataframe header and row 
    dtf = pd.DataFrame(new_df_row.values[0:], columns = new_df_col.iloc[0])

    # Restructuring Date and Time 
    date_hour = dt_hr(dtf, start_point)

    # Calculate running_balance and group name assigning
    initial_bal_check = int_bal_ck(start_point)
    final_bal_check = finl_bal_ck(end_point)

    bal_grp = run_bal_grp(dtf, initial_bal_check, final_bal_check)
    print(bal_grp)

    # Restructuring Référence column data

    txn = restr_txn(dtf, start_point)

    # # Add columns and replace columns to the new dataframe
    tdf = dtf.assign(Date = date_hour[0], Heure = date_hour[1], Référence = txn, balance = bal_grp[0], group = bal_grp[1], amount = bal_grp[2], stmt_txn_type = bal_grp[3])

    return tdf

try:
    # global driver
    # driver = initialize()
    # driver.get('https://madagascar.orange-money.com/grweb/')
    # #search = driver.find_element(by=By.NAME, value="q")
    # #search.send_keys("Hey, Tecadmin")
    # #search.send_keys(Keys.RETURN)
    # username = find_if_exists_by_xpath("//input[contains(@id, 'login_dologin_loginId')]")
    # password = find_if_exists_by_xpath("//input[contains(@id, 'login_dologin_password')]")
    # username.send_keys("CU_WS2_STEIJOHAN")
    # password.send_keys("Fow@37")

    # find_if_exists_by_xpath("//input[contains(@id, 'login_dologin_0')]").click()
    # find_if_exists_by_xpath("//ul/table[5]/tbody/tr/td/li[contains(@class, 'fol')]/a").click();
    # find_if_exists_by_xpath("//ul/table[1]/tbody/tr/td/li[contains(@class, 'excel')]/a").click();
    # sleep(8)
    # initial_path = "/home/oem/Downloads"
    # filename = max([initial_path + "/" + f for f in os.listdir(initial_path)],key=os.path.getctime)
    # print(filename)
    df = pd.read_excel("/home/oem/Downloads/Daily-ChannelUserTransactionReport-0322285302-20220822.xls")
    acc_no = "0322284219"
    # table_df = table_data(acc_no)
    # db_transform(table_df)
    # dh = df.apply(lambda row: row.astype(str).str.contains(acc_no).any(), axis=1)
    # dt = dh.iloc[65:,]
    # rows = len(df.axes[0])
    # print(rows)
    compte = []
    agent = []
    for x in df.itertuples():
        compte_check = x._1 == "Compte Orange Money :"
        agent_check = x._1 == "Agent"
        if compte_check == True:
            compte.append(x.Index)
            # print(x.Index, x._4)
        elif agent_check == True:
            agent.append(x.Index)
        
    print(compte)
    print(agent)


    for each_acc_ind in compte:
        if each_acc_ind == 13: 
            
            td_data = table_data(each_acc_ind)
            print(td_data)
            db_transform(td_data)
            break
        # else:
        #     "Master does not will be inserted"
        
        # if df[x].str.contains("Compte Orange Money :"):
        #     print(x)
    # ed_loc = df.loc[df["Compte Orange Money :"].str.contains("A", case=False, na=False)]
    # print(ed_loc)

        # if acc_no == dh:
        #     print("fdgg", dh)
        #     break
    # print(dh)


except Exception as e:
    print('Exception')
    print(e)
# finally:
    # sleep(20)
    # driver.close()