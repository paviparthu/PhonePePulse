import pandas as pd              #aggregated trans-->1
import json
import os
from db_connect import *
def Agg_Tran():
    path="e:/PAVITHRA/pavi_phonepe/Phonepe_Pulse/data/aggregated/transaction/country/india/state/"
    Agg_state_list=os.listdir(path)
    Agg_state_list

    clm={'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}
    for i in Agg_state_list:
        p_i=path+i+"/"
        Agg_yr=os.listdir(p_i)
        for j in Agg_yr:
            p_j=p_i+j+"/"
            Agg_yr_list=os.listdir(p_j)
            for k in Agg_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D=json.load(Data)
                for z in D['data']['transactionData']:
                    Name=z['name']
                    count=z['paymentInstruments'][0]['count']
                    amount=z['paymentInstruments'][0]['amount']
                    clm['Transacion_type'].append(Name)
                    clm['Transacion_count'].append(count)
                    clm['Transacion_amount'].append(amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))
    #Succesfully created a dataframe
    Agg_Trans=pd.DataFrame(clm) 
    Agg_Trans_insertvalues(Agg_Trans)

    
def Agg_User():
    path="E:/PAVITHRA/pavi_phonepe/Phonepe_Pulse/data/aggregated/user/country/india/state/"  #agg user-->2
    Agg_state_list=os.listdir(path)


    clm={'State':[], 'Year':[],'Quater':[],'brand':[], 'count':[], 'percentage':[]}

    for a in Agg_state_list:
        p_a=path+a+"/"
        Agg_yr=os.listdir(p_a)
        for b in Agg_yr:
            p_b=p_a+b+"/"
            Agg_yr_list=os.listdir(p_b)
            for c in Agg_yr_list:
                p_c=p_b+c
                Data=open(p_c,'r')
                D=json.load(Data)
                DataList = D['data']['usersByDevice']
                if DataList:
                    for e in DataList:
                        brand=e['brand']
                        count=e['count']
                        percentage=e['percentage']
                        clm['brand'].append(brand)
                        clm['count'].append(count)
                        clm['percentage'].append(percentage)
                        clm['State'].append(a)
                        clm['Year'].append(b)
                        clm['Quater'].append(int(c.strip('.json')))
    #Succesfully created a dataframe
    Agg_User=pd.DataFrame(clm)
    Agg_Users_insertvalues(Agg_User)
    

def Map_Trans():
    path="E:/PAVITHRA/pavi_phonepe/Phonepe_Pulse/data/map/transaction/hover/country/india/state/"
    Agg_state_list=os.listdir(path)

    clm={'State':[], 'Year':[],'Quater':[],'name':[], 'metric_count':[], 'metric_amount':[]}
    for i in Agg_state_list:
        p_i=path+i+"/"
        Agg_yr=os.listdir(p_i)
        for j in Agg_yr:
            p_j=p_i+j+"/"
            Agg_yr_list=os.listdir(p_j)
            for k in Agg_yr_list:
                p_k=p_j+k

                Data=open(p_k,'r')
                D=json.load(Data)

                for z in D['data']['hoverDataList']:
                    Name=z['name']
                    count=z['metric'][0]['count']
                    amount=z['metric'][0]['amount']
                    clm['name'].append(Name)
                    clm['metric_count'].append(count)
                    clm['metric_amount'].append(amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))
    #Succesfully created a dataframe
    Map_Trans=pd.DataFrame(clm)
    Map_Trans_insertvalues(Map_Trans)
def Map_User():
    user_path="E:/PAVITHRA/pavi_phonepe/Phonepe_Pulse/data/map/user/hover/country/india/state/"
    state_list=os.listdir(user_path)

    column={'State':[],'Year':[],'Quater':[],'districts':[],'RegisteredUsers':[],'AppOpens':[]}

    for i in state_list:
        p_i=user_path+"/"+i+"/"
        user_year=os.listdir(p_i)
        for j in user_year:
            p_j=p_i+j+"/"
            user_year_list=os.listdir(p_j)
            for k in user_year_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D=json.load(Data)
                for z in D['data']['hoverData']:
                    name=z
                    reg_users = D['data']['hoverData'][z]['registeredUsers']
                    app_open = D['data']['hoverData'][z]['appOpens']
                    column['districts'].append(name)
                    column['RegisteredUsers'].append(reg_users)
                    column['AppOpens'].append(app_open)
                    column['State'].append(i)
                    column['Year'].append(j)
                    column['Quater'].append(int(k.strip('.json')))
        #Succesfully created a dataframe
    Map_User=pd.DataFrame(column)
    Map_Users_insertvalues(Map_User)
    
def Top_Trans():
    path="E:/PAVITHRA/pavi_phonepe/Phonepe_Pulse/data/top/transaction/country/india/state/"
    top_state_list=os.listdir(path)
    column={'State':[],'Year':[],'Quater':[],'Name':[],'Metric_count':[],'Metric_amount':[]}

    for i in top_state_list:
        p_i=path+i+"/"
        trans_year=os.listdir(p_i)
        for j in trans_year:
            p_j=p_i+j+"/"
            trans_year_list=os.listdir(p_j)
            for k in trans_year_list:
                p_k=p_j+k

                Data=open(p_k,'r')
                D=json.load(Data)
                DataList = D['data']

                for z in D['data']['districts']:
                    name=z['entityName']
                    count=z['metric']['count']
                    amount=z['metric']['amount']
                    column['Name'].append(name)
                    column['Metric_count'].append(count)
                    column['Metric_amount'].append(amount)
                    column['State'].append(i)
                    column['Year'].append(j)
                    column['Quater'].append(int(k.strip('.json')))
                for z in D['data']['pincodes']:
                    name=z['entityName']
                    count=z['metric']['count']
                    amount=z['metric']['amount']
                    column['Name'].append(name)
                    column['Metric_count'].append(count)
                    column['Metric_amount'].append(amount)
                    column['State'].append(i)
                    column['Year'].append(j)
                    column['Quater'].append(int(k.strip('.json')))


    #Succesfully created a dataframe
    Top_Trans=pd.DataFrame(column)
    Top_Trans_insertvalues((Top_Trans))
    
def Top_User():
    path="E:/PAVITHRA/pavi_phonepe/Phonepe_Pulse/data/top/user/country/india/state/"
    top_state_list=os.listdir(path)
    

    clm={'State':[],'Year':[],'Quater':[],'district':[],'RegisteredUsers':[]}
    for a in top_state_list:
        p_a=path+a+"/"
        top_yr=os.listdir(p_a)
        for b in top_yr:
            p_b=p_a+b+"/"
            top_yr_list=os.listdir(p_b)
            for c in top_yr_list:
                p_c=p_b+c
                Data=open(p_c,'r')
                D=json.load(Data)
                DataList = D['data']['districts']
                if DataList:
                    for e in DataList:
                        name=e['name']
                        reg_users=e['registeredUsers']
                        clm['district'].append(name)
                        clm['RegisteredUsers'].append(reg_users)
                        clm['State'].append(a)
                        clm['Year'].append(b)
                        clm['Quater'].append(int(c.strip('.json')))
    #Succesfully created a dataframe
    Top_User=pd.DataFrame(clm)
    top_Users_insertvalues(Top_User)


       

        
    

connectOpen()
createTbl()
Agg_Tran()
Agg_User()
Map_Trans()
Map_User()
Top_Trans()
Top_User()
connectClose()
