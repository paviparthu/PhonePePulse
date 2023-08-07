import streamlit as st
import plotly.express as px
import pandas as pd
from db_connect import *
from paviphonepe import *
from parse import *
connectOpen()
st.title('PHONEPE_PULSE')
if st.button('Refresh Data'):
    cloneRepo()
    populateData()
states_record = get_states("aggregated_transaction")
states_list = []
for row in states_record:
    states_list.append(row[0])
    
years_record = get_years("aggregated_transaction")
years_list = []
for row in years_record:
    years_list.append(row[0])    
#agg_trans
state_txt = st.selectbox('State',states_list)
year_txt = st.selectbox('Year',years_list)
quater_txt = st.selectbox('Quater',['None',1,2,3,4])
option_txt = st.selectbox('Options',['None','Show Pie chart for transaction count','Show Pie chart for transaction amount','Show Pie chart for brandcount',
                            'Show Pie chart for District metric count', 'Show Pie chart for Registered Users'])
st.write(option_txt)
if str(option_txt) == str("Show Pie chart for transaction count"):
    tran_result = get_agg_Transactions(state_txt, year_txt, quater_txt)
    df = pd.DataFrame(tran_result, columns=['TranType', 'TranCount', 'TranAmount'])
    print(df)
    fig = px.pie(df, values='TranCount', names ='TranType', height=300, width=200)
    st.plotly_chart(fig, use_container_width=True)
    
elif str(option_txt) == str("Show Pie chart for transaction amount"):
    tran_result = get_agg_Transactions(state_txt, year_txt, quater_txt)
    df = pd.DataFrame(tran_result, columns=['TranType',  'TranCount', 'TranAmount'])
    print(df)
    fig = px.pie(df, values='TranAmount', names ='TranType', height=300, width=200)
    st.plotly_chart(fig, use_container_width=True)
elif str(option_txt) == str("Show Pie chart for brandcount"):
    user_result = get_agg_Users(state_txt, year_txt, quater_txt)
    df = pd.DataFrame(user_result, columns=['Brand',  'Count', 'Percentage'])
    print(df)
    fig = px.pie(df, values='Count', names ='Brand', height=400, width=200)
    st.plotly_chart(fig, use_container_width=True)
elif str(option_txt) == str("Show Pie chart for District metric count"):
    map_result = get_map_Transactions(state_txt, year_txt, quater_txt)
    df = pd.DataFrame(map_result, columns=['Name',  'Metric_Count', 'Metric_Amount'])
    print(df)
    fig = px.pie(df, values='Metric_Count', names ='Name', height=300, width=200)
    st.plotly_chart(fig, use_container_width=True)
elif str(option_txt) == str("Show Pie chart for Registered Users"):
    map_result = get_map_Users(state_txt, year_txt, quater_txt)
    df = pd.DataFrame(map_result, columns=['Districts',  'RegisteredUsers', 'AppOpens'])
    print(df)
    fig = px.pie(df, values='RegisteredUsers', names ='Districts', height=400, width=200)
    st.plotly_chart(fig, use_container_width=True)



    

#fig.show()
#st.write('selected value:',state_txt)