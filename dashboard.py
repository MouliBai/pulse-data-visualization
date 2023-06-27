import streamlit as st
st.set_page_config(layout='wide')

import base64
import requests
import subprocess
import pandas as pd
import numpy as np
import os
import json
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
import plotly.express as px
df = px.data.iris()

@st.cache_data

def get_img_as_base64(file):
    with open(file, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()


img = get_img_as_base64("your_image_url.jpg")

page_bg_img = f"""
<style>
[data-testid="stAppViewContainer"] > .main {{
background-image: url("https://images.unsplash.com/photo-1615715616181-6ba85d724137?ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D&auto=format&fit=crop&w=387&q=80");
background-size: 180%;
background-position: top left;
background-repeat: no-repeat;
background-attachment: local;
}}

[data-testid="stSidebar"] > div:first-child {{
background-image: url("data:image/png;base64,{img}");
background-position: center; 
background-repeat: no-repeat;
background-attachment: fixed;
}}

[data-testid="stHeader"] {{
background: rgba(0,0,0,0);
}}

[data-baseweb="tab"] {{
background: rgba(0,0,0,0);
}}


[data-testid="stToolbar"] {{
right: 2rem;
}}
</style>
"""
st.markdown(page_bg_img, unsafe_allow_html=True)

conn = pymysql.connect(host='localhost', user='root', password='1234', db='phonepe_pulse')
cursor = conn.cursor()

st.title('Phonepe Pulse Data Visualization')

in_tr_yr = st.sidebar.selectbox('**Select Year**', ('2018','2019','2020','2021','2022'),key='in_tr_yr')

in_tr_qtr = st.sidebar.selectbox("**Select Quarter**",('1','2','3','4'),key='in_tr_qtr')
st.sidebar.write("**Q1**: January 1 – March 31")
st.sidebar.write("**Q2**: April 1 – June 3")
st.sidebar.write("**Q3**: July 1 – September 30")
st.sidebar.write("**Q4**: October 1 – December 31")
tab1, tab2 = st.tabs(['Transaction','User'])
with tab1:
    in_tr_tr_typ = st.selectbox('**Select Transaction type**', ('Recharge & bill payments','Peer-to-peer payments','Merchant payments','Financial Services','Others'),key='in_tr_tr_typ')
    # Transaction Analysis bar chart query
    cursor.execute(f"SELECT State, Transaction_amount FROM aggregated_transaction WHERE Year = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
    in_tr_tab_qry_rslt = cursor.fetchall()
    df_in_tr_tab_qry_rslt = pd.DataFrame(np.array(in_tr_tab_qry_rslt), columns=['State', 'Transaction_amount'])
    df_in_tr_tab_qry_rslt1 = df_in_tr_tab_qry_rslt.set_index(pd.Index(range(1, len(df_in_tr_tab_qry_rslt)+1)))

    # Transaction Analysis table query
    cursor.execute(f"SELECT State, Transaction_count, Transaction_amount FROM aggregated_transaction WHERE Year = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
    in_tr_anly_tab_qry_rslt = cursor.fetchall()
    df_in_tr_anly_tab_qry_rslt = pd.DataFrame(np.array(in_tr_anly_tab_qry_rslt), columns=['State','Transaction_count','Transaction_amount'])
    df_in_tr_anly_tab_qry_rslt1 = df_in_tr_anly_tab_qry_rslt.set_index(pd.Index(range(1, len(df_in_tr_anly_tab_qry_rslt)+1)))

    # Total Transaction Amount table query
    cursor.execute(f"SELECT SUM(Transaction_amount), AVG(Transaction_amount) FROM aggregated_transaction WHERE Year = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
    in_tr_am_qry_rslt = cursor.fetchall()
    df_in_tr_am_qry_rslt = pd.DataFrame(np.array(in_tr_am_qry_rslt), columns=['Total','Average'])
    df_in_tr_am_qry_rslt1 = df_in_tr_am_qry_rslt.set_index(['Average'])
    
    # Total Transaction Count table query
    cursor.execute(f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_transaction WHERE Year = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
    in_tr_co_qry_rslt = cursor.fetchall()
    df_in_tr_co_qry_rslt = pd.DataFrame(np.array(in_tr_co_qry_rslt), columns=['Total','Average'])
    df_in_tr_co_qry_rslt1 = df_in_tr_co_qry_rslt.set_index(['Average'])

    # --------- / Output  /  -------- #
    # Drop a State column from df_in_tr_tab_qry_rslt
    df_in_tr_tab_qry_rslt.drop(columns=['State'], inplace=True)
    # Clone the gio data
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)
    # Extract state names and sort them in alphabetical order
    state_names_tra = [feature['properties']['ST_NM'] for feature in data1['features']]
    state_names_tra.sort()
    # Create a DataFrame with the state names column
    df_state_names_tra = pd.DataFrame({'State': state_names_tra})
    # Combine the Gio State name with df_in_tr_tab_qry_rslt
    df_state_names_tra['Transaction_amount']=df_in_tr_tab_qry_rslt
    # convert dataframe to csv file
    df_state_names_tra.to_csv('State_trans.csv', index=False)
    # Read csv
    df_tra = pd.read_csv('State_trans.csv')
    # Geo plot
    fig_tra = px.choropleth(
        df_tra,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',locations='State',color='Transaction_amount',color_continuous_scale='thermal',title = 'Transaction Analysis')
    fig_tra.update_geos(fitbounds="locations", visible=False)
    fig_tra.update_layout(title_font=dict(size=33),title_font_color='#6739b7', height=700,paper_bgcolor="rgba(0, 0, 0, 0)")
    st.plotly_chart(fig_tra,use_container_width=True)

    # ---------   /   All India Transaction Analysis Bar chart  /  ----- #
    df_in_tr_tab_qry_rslt1['State'] = df_in_tr_tab_qry_rslt1['State'].astype(str)
    df_in_tr_tab_qry_rslt1['Transaction_amount'] = df_in_tr_tab_qry_rslt1['Transaction_amount'].astype(float)
    df_in_tr_tab_qry_rslt1_fig = px.bar(df_in_tr_tab_qry_rslt1 , x = 'State', y ='Transaction_amount', color ='Transaction_amount', color_continuous_scale = 'thermal', title = 'Transaction Analysis Chart', height = 800,)
    df_in_tr_tab_qry_rslt1_fig.update_layout(title_font=dict(size=34),title_font_color='#6739b7',paper_bgcolor="rgba(0, 0, 0, 0)")
    df_in_tr_tab_qry_rslt1_fig.update_layout(xaxis=dict(tickfont=dict(color='black'),title_font=dict(color='black')),yaxis=dict(tickfont=dict(color='black')))

    st.plotly_chart(df_in_tr_tab_qry_rslt1_fig,use_container_width=True)

    # -------  /  All India Total Transaction calculation Table   /   ----  #
    st.header(':violet[Total calculation]')

    st.subheader('Transaction Analysis')
    st.dataframe(df_in_tr_anly_tab_qry_rslt1, width=None)
    st.subheader('Transaction Amount')
    st.dataframe(df_in_tr_am_qry_rslt1, width=None)
    st.subheader('Transaction Count')
    st.dataframe(df_in_tr_co_qry_rslt1)
