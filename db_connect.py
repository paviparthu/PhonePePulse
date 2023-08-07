import psycopg2

conn = None
cur = None
def connectOpen():
    try:
        hostname = 'localhost'
        database = 'phonepe_pulse'
        username = 'postgres'
        port_id = 5432
        pwd = 'root'
        global conn
        global cur
        conn = psycopg2.connect(
                    host = hostname,
                    dbname = database,
                    user = username,
                    password = pwd,
                    port = port_id)                  
        cur = conn.cursor()
        
    except Exception as error:
        print(error)  
    
    
def createTbl():
    global cur
    global conn
    cur.execute('DROP TABLE IF EXISTS aggregated_transaction')
    create_script_agg_trans = '''CREATE TABLE IF NOT EXISTS aggregated_transaction(
                            State text,
                            Year int,
                            Quater int,
                            Transaction_type VARCHAR(50),
                            Transaction_count int,
                            Transaction_amount float)'''
    cur.execute(create_script_agg_trans)  

    cur.execute('DROP TABLE IF EXISTS aggregated_users')
    create_script_agg_users = '''CREATE TABLE IF NOT EXISTS aggregated_users(
                            State text,
                            Year int,
                            Quater int,
                            Brand VARCHAR(20),
                            Count int,
                            Percentage float)'''
    cur.execute(create_script_agg_users)  
  
    cur.execute('DROP TABLE IF EXISTS map_transaction')
    create_script_map_trans = '''CREATE TABLE IF NOT EXISTS map_transaction(
                            State text,
                            Year int,
                            Quater int,
                            Name VARCHAR(50),
                            Metric_count int,
                            Metric_amount float)'''
    cur.execute(create_script_map_trans)  
    
    cur.execute('DROP TABLE IF EXISTS map_users')
    create_script_map_users = '''CREATE TABLE IF NOT EXISTS map_users(
                            State text,
                            Year int,
                            Quater int,
                            Districts VARCHAR(50),
                            RegisteredUsers int,
                            AppOpens int)'''
    cur.execute(create_script_map_users)                         
    
    cur.execute('DROP TABLE IF EXISTS  top_transaction')
    create_script_top_trans = '''CREATE TABLE IF NOT EXISTS top_transaction(
                            State text,
                            Year int,
                            Quater int,
                            Name VARCHAR(50),
                            Metric_count int,
                            Metric_amount float)'''
    cur.execute(create_script_top_trans)
    
    cur.execute('DROP TABLE IF EXISTS  top_users')
    create_script_top_users = '''CREATE TABLE IF NOT EXISTS top_users(
                            State text,
                            Year int,
                            Quater int,
                            District VARCHAR(50),
                            RegisteredUsers int)'''
    cur.execute(create_script_top_users)
    conn.commit()

    

    
    
def Agg_Trans_insertvalues(agg_trans_df):
    global cur
    global conn 
    for index,row in agg_trans_df.iterrows():
        insert_script = '''INSERT INTO aggregated_transaction(state,year,quater,Transaction_type,Transaction_count,Transaction_amount) VALUES(%s,%s,%s,%s,%s,%s)'''
        cur.execute(insert_script,(row))
    conn.commit()
    
def Agg_Users_insertvalues(agg_users_df):
    global cur
    global conn 
    for index,row in agg_users_df.iterrows():
        insert_script = '''INSERT INTO aggregated_users(state,year,quater,brand,count,percentage) VALUES(%s,%s,%s,%s,%s,%s)'''
        cur.execute(insert_script,(row))
    conn.commit()
    
def Map_Trans_insertvalues(map_trans_df):
    global cur
    global conn 
    for index,row in map_trans_df.iterrows():
        insert_script = '''INSERT INTO map_transaction(state,year,quater,name,metric_count,metric_amount) VALUES(%s,%s,%s,%s,%s,%s)'''
        cur.execute(insert_script,(row))
    conn.commit()
    
def Map_Users_insertvalues(map_users_df):
    global cur
    global conn 
    for index,row in map_users_df.iterrows():
        insert_script = '''INSERT INTO map_users(state,year,quater,districts,registeredusers,appopens) VALUES(%s,%s,%s,%s,%s,%s)'''
        cur.execute(insert_script,(row))
    conn.commit()
    
def Top_Trans_insertvalues(top_trans_df):
    global cur
    global conn 
    for index,row in top_trans_df.iterrows():
        insert_script = '''INSERT INTO top_transaction(state,year,quater,name,metric_count,metric_amount) VALUES(%s,%s,%s,%s,%s,%s)'''
        cur.execute(insert_script,(row))
    conn.commit()
    
def top_Users_insertvalues(top_users_df):
    global cur
    global conn 
    for index,row in top_users_df.iterrows():
        insert_script = '''INSERT INTO top_users(state,year,quater,district,registeredusers) VALUES(%s,%s,%s,%s,%s)'''
        cur.execute(insert_script,(row))
    conn.commit()
    
    
       


     
def get_states(Table_name):
    global cur
    global conn 
    cur.execute(f'select distinct state from {Table_name} order by state asc')
    record = cur.fetchall()
    return record   
    conn.commmit()
    
def get_years(Table_name):
    global cur
    global conn 
    cur.execute(f'select distinct year from {Table_name} order by year asc')
    record = cur.fetchall()
    return record   
    conn.commmit()
    
    
def get_agg_Transactions(state, year, quater):    #agg_trans
    global cur
    global conn 
    if quater == 'None':
        cur.execute('select transaction_type, transaction_count, transaction_amount from aggregated_transaction where state = %s and year = %s', (state, year))
    else:
        cur.execute('select transaction_type, transaction_count, transaction_amount from aggregated_transaction where state = %s and year = %s and quater = %s', (state, year, quater))
    record = cur.fetchall()
    return record   
    conn.commmit()
def get_agg_Users(state, year, quater):   #agg_users
    global cur
    global conn
    if quater =='None':
        cur.execute('select brand,count,percentage from aggregated_users where state = %s and year = %s',(state, year))
    else:     
        cur.execute('select brand,count,percentage from aggregated_users where state = %s and year = %s and quater = %s', (state, year, quater))
    record = cur.fetchall()
    return record   
    conn.commmit()
def get_map_Transactions(state, year, quater):     #map_trans
    global cur
    global conn 
    if quater =='None':
        cur.execute('select name, metric_count, metric_amount from map_transaction where state = %s and year = %s ', (state, year))
    else:
        cur.execute('select name, metric_count, metric_amount from map_transaction where state = %s and year = %s and quater = %s', (state, year, quater))
    record = cur.fetchall()
    return record   
    conn.commmit()
def get_map_Users(state, year, quater):     #map_users
    global cur
    global conn 
    if quater =='None':
        cur.execute('select districts, registeredusers, appopens from map_users where state = %s and year = %s ', (state, year))
    else:   
        cur.execute('select districts, registeredusers, appopens from map_users where state = %s and year = %s and quater = %s', (state, year, quater))
    record = cur.fetchall()
    return record   
    conn.commmit()
def get_top_Transactions(state, year, quater):     #top_trans
    global cur
    global conn
    if quater =='None':
        cur.execute('select name, metric_count, metric_amount from top_transaction where state = %s and year = %s ', (state, year))    
    else:
        cur.execute('select name, metric_count, metric_amount from top_transaction where state = %s and year = %s and quater = %s', (state, year, quater))
    record = cur.fetchall()
    return record   
    conn.commmit()
def get_top_Users(state, year, quater):     #top_usrs
    global cur
    global conn 
    cur.execute('select district, registeredusers, from top_transaction where state = %s and year = %s and quater = %s', (state, year, quater))
    record = cur.fetchall()
    return record   
    conn.commmit()


def connectClose():
    global cur
    global conn
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
        
def displayTxt():
    print("hello")