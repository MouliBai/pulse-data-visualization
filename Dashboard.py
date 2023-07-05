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
tab1, tab2 , tab3 = st.tabs(['Transaction','User','Top10'])
with tab1:
    in_tr_tr_typ = st.selectbox(':red[**Select Transaction type**]', ('Recharge & bill payments','Peer-to-peer payments','Merchant payments','Financial Services','Others'),key='in_tr_tr_typ')

    cursor.execute(f"SELECT State, Transaction_amount FROM aggregated_transaction WHERE Year = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
    in_tr_tab_qry_rslt = cursor.fetchall()
    df_in_tr_tab_qry_rslt = pd.DataFrame(np.array(in_tr_tab_qry_rslt), columns=['State', 'Transaction_amount'])
    df_in_tr_tab_qry_rslt1 = df_in_tr_tab_qry_rslt.set_index(pd.Index(range(1, len(df_in_tr_tab_qry_rslt)+1)))

    cursor.execute(f"SELECT State, Transaction_count, Transaction_amount FROM aggregated_transaction WHERE Year = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
    in_tr_anly_tab_qry_rslt = cursor.fetchall()
    df_in_tr_anly_tab_qry_rslt = pd.DataFrame(np.array(in_tr_anly_tab_qry_rslt), columns=['State','Transaction_count','Transaction_amount'])
    df_in_tr_anly_tab_qry_rslt1 = df_in_tr_anly_tab_qry_rslt.set_index(pd.Index(range(1, len(df_in_tr_anly_tab_qry_rslt)+1)))

    cursor.execute(f"SELECT SUM(Transaction_amount), AVG(Transaction_amount) FROM aggregated_transaction WHERE Year = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
    in_tr_am_qry_rslt = cursor.fetchall()
    df_in_tr_am_qry_rslt = pd.DataFrame(np.array(in_tr_am_qry_rslt), columns=['Total','Average'])
    df_in_tr_am_qry_rslt1 = df_in_tr_am_qry_rslt.set_index(['Total'])
    

    cursor.execute(f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_transaction WHERE Year = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
    in_tr_co_qry_rslt = cursor.fetchall()
    df_in_tr_co_qry_rslt = pd.DataFrame(np.array(in_tr_co_qry_rslt), columns=['Total','Average'])
    df_in_tr_co_qry_rslt1 = df_in_tr_co_qry_rslt.set_index(['Total'])

    df_in_tr_tab_qry_rslt.drop(columns=['State'], inplace=True)

    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)

    state_names_tra = [feature['properties']['ST_NM'] for feature in data1['features']]
    state_names_tra.sort()

    df_state_names_tra = pd.DataFrame({'State': state_names_tra})

    df_state_names_tra['Transaction_amount']=df_in_tr_tab_qry_rslt

    df_state_names_tra.to_csv('State_trans.csv', index=False)

    df_tra = pd.read_csv('State_trans.csv')

    fig_tra = px.choropleth(
        df_tra,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',locations='State',color='Transaction_amount',color_continuous_scale='thermal',title = 'Transaction Analysis')
    fig_tra.update_geos(fitbounds="locations", visible=False)
    fig_tra.update_layout(title_font=dict(size=33),title_font_color='#6739b7', height=700,paper_bgcolor="rgba(0, 0, 0, 0)")
    st.plotly_chart(fig_tra,use_container_width=True)

    st.title('Total Transaction Statewise')

    query1 = f"SELECT state ,sum(Transaction_amount) as Transaction_amount FROM aggregated_transaction WHERE quarter = '{in_tr_qtr}' AND year = '{in_tr_yr}' AND Transaction_type = '{in_tr_tr_typ}' group by state order by Transaction_amount desc"
    cursor.execute(query1)
    result1 = cursor.fetchall()
    df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
    fig = px.bar(df1,
    title = 'Amount',
    x="state",
    y="Transaction_amount",
    orientation='v',
    color='Transaction_amount',
    color_continuous_scale='Blues')
    fig.update_layout(title_font=dict(size=34),title_font_color='#6739b7',paper_bgcolor="rgba(0, 0, 0, 0)")
    fig.update_layout(xaxis=dict(tickfont=dict(color='black'),title_font=dict(color='black')),yaxis=dict(tickfont=dict(color='black')))
    #st.code(df1)
    st.plotly_chart(fig,use_container_width=True)

    query2 = f"SELECT state ,sum(Transaction_count) as Transaction_count FROM aggregated_transaction WHERE quarter = '{in_tr_qtr}' AND year = '{in_tr_yr}' AND Transaction_type = '{in_tr_tr_typ}' group by state order by Transaction_count desc"
    cursor.execute(query2)
    result1 = cursor.fetchall()
    df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
    fig = px.bar(df1,
    title = 'Counts',
    x="state",
    y="Transaction_count",
    orientation='v',
    color='Transaction_count',
    color_continuous_scale='Blues')
    fig.update_layout(title_font=dict(size=34),title_font_color='#6739b7',paper_bgcolor="rgba(0, 0, 0, 0)")
    fig.update_layout(xaxis=dict(tickfont=dict(color='black'),title_font=dict(color='black')),yaxis=dict(tickfont=dict(color='black')))
    #st.code(df1)
    st.plotly_chart(fig,use_container_width=True)

    query1 = f"SELECT Transaction_type,sum(Transaction_count) as count FROM aggregated_transaction WHERE quarter = '{in_tr_qtr}' AND year = '{in_tr_yr}' group by Transaction_type order by count desc "
    cursor.execute(query1)
    result1 = cursor.fetchall()
    df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
    fig = px.bar(df1,
    title='Total Transaction Category wise' ,
    x="Transaction_type",
    y="count",
    orientation='v',
    color='count',
    color_continuous_scale=px.colors.sequential.Magenta)
    fig.update_layout(title_font=dict(size=34),title_font_color='#6739b7',paper_bgcolor="rgba(0, 0, 0, 0)")
    fig.update_layout(xaxis=dict(tickfont=dict(color='black'),title_font=dict(color='black')),yaxis=dict(tickfont=dict(color='black')))
    st.plotly_chart(fig,use_container_width=True)

    st.header(':violet[Total calculation]')
    col1, col2 = st.columns(2)
    with col1:
        st.subheader('Transaction Analysis')
        st.dataframe(df_in_tr_anly_tab_qry_rslt1, width=None)
    with col2:
        st.subheader('Transaction Amount')
        st.dataframe(df_in_tr_am_qry_rslt1, width=None)
        st.subheader('Transaction Count')
        st.dataframe(df_in_tr_co_qry_rslt1)


with tab2:
    cursor.execute(f"SELECT State, SUM(User_Count) FROM aggregated_user WHERE Year = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' GROUP BY State;")
    in_us_tab_qry_rslt = cursor.fetchall()
    df_in_us_tab_qry_rslt = pd.DataFrame(np.array(in_us_tab_qry_rslt), columns=['State', 'User Count'])
    df_in_us_tab_qry_rslt1 = df_in_us_tab_qry_rslt.set_index(pd.Index(range(1, len(df_in_us_tab_qry_rslt)+1)))

    cursor.execute(f"SELECT SUM(User_Count), AVG(User_Count) FROM aggregated_user WHERE Year = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}';")
    in_us_co_qry_rslt = cursor.fetchall()
    df_in_us_co_qry_rslt = pd.DataFrame(np.array(in_us_co_qry_rslt), columns=['Total','Average'])
    df_in_us_co_qry_rslt1 = df_in_us_co_qry_rslt.set_index(['Average'])

    df_in_us_tab_qry_rslt.drop(columns=['State'], inplace=True)

    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data2 = json.loads(response.content)

    state_names_use = [feature['properties']['ST_NM'] for feature in data2['features']]
    state_names_use.sort()

    df_state_names_use = pd.DataFrame({'State': state_names_use})

    df_state_names_use['User Count']=df_in_us_tab_qry_rslt

    df_state_names_use.to_csv('State_user.csv', index=False)

    df_use = pd.read_csv('State_user.csv')

    fig_use = px.choropleth(
        df_use,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',locations='State',color='User Count',color_continuous_scale='thermal',title = 'User Analysis')
    fig_use.update_geos(fitbounds="locations", visible=False)
    fig_use.update_layout(title_font=dict(size=33),title_font_color='#6739b7', height=700,paper_bgcolor="rgba(0, 0, 0, 0)")
    st.plotly_chart(fig_use,use_container_width=True)

    df_in_us_tab_qry_rslt1['State'] = df_in_us_tab_qry_rslt1['State'].astype(str)
    df_in_us_tab_qry_rslt1['User Count'] = df_in_us_tab_qry_rslt1['User Count'].astype(int)
    df_in_us_tab_qry_rslt1_fig = px.bar(df_in_us_tab_qry_rslt1 , x = 'State', y ='User Count', color ='User Count', color_continuous_scale = 'thermal', title = 'User Analysis Chart', height = 700,)
    df_in_us_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7',paper_bgcolor="rgba(0, 0, 0, 0)")
    df_in_us_tab_qry_rslt1_fig.update_layout(xaxis=dict(tickfont=dict(color='black'),title_font=dict(color='black')),yaxis=dict(tickfont=dict(color='black')))
    st.plotly_chart(df_in_us_tab_qry_rslt1_fig,use_container_width=True)
    
    st.header(':violet[Total calculation]')
    st.subheader('User Analysis')
    st.dataframe(df_in_us_tab_qry_rslt1)
    st.subheader('User Count')
    st.dataframe(df_in_us_co_qry_rslt1)

with tab3:

    option = st.radio('**Select your option**',('State', 'Districts','Pincodes'),horizontal=True)
    if option == 'State':
        st.write(':red[*Select only Year in sidepanel]')
        st.title('Top 10 Analysis of both Transaction and User')
        
        cursor.execute(f"SELECT State, SUM(Transaction_amount) As Transaction_amount FROM top_transaction WHERE Year = '{in_tr_yr}' GROUP BY State ORDER BY Transaction_amount DESC LIMIT 10;")
        top_tr_tab_qry_rslt = cursor.fetchall()
        df_top_tr_tab_qry_rslt = pd.DataFrame(np.array(top_tr_tab_qry_rslt), columns=['State', 'Top Transaction amount'])
        df_top_tr_tab_qry_rslt1 = df_top_tr_tab_qry_rslt.set_index(pd.Index(range(1, len(df_top_tr_tab_qry_rslt)+1)))

        cursor.execute(f"SELECT State, SUM(Registered_User) AS Top_user FROM top_user WHERE Year='{in_tr_yr}' GROUP BY State ORDER BY Top_user DESC LIMIT 10;")
        top_us_tab_qry_rslt = cursor.fetchall()
        df_top_us_tab_qry_rslt = pd.DataFrame(np.array(top_us_tab_qry_rslt), columns=['State', 'Total User count'])
        df_top_us_tab_qry_rslt1 = df_top_us_tab_qry_rslt.set_index(pd.Index(range(1, len(df_top_us_tab_qry_rslt)+1)))

        df_top_tr_tab_qry_rslt1['State'] = df_top_tr_tab_qry_rslt1['State'].astype(str)
        df_top_tr_tab_qry_rslt1['Top Transaction amount'] = df_top_tr_tab_qry_rslt1['Top Transaction amount'].astype(float)
        df_top_tr_tab_qry_rslt1_fig = px.bar(df_top_tr_tab_qry_rslt1 , x = 'State', y ='Top Transaction amount', color ='Top Transaction amount', color_continuous_scale = 'thermal', title = 'Top 10 Transaction Chart', height = 600,)
        df_top_tr_tab_qry_rslt1_fig.update_layout(title_font=dict(size=34),title_font_color='#6739b7',paper_bgcolor="rgba(0, 0, 0, 0)")
        df_top_tr_tab_qry_rslt1_fig.update_layout(xaxis=dict(tickfont=dict(color='black'),title_font=dict(color='black')),yaxis=dict(tickfont=dict(color='black')))
        st.plotly_chart(df_top_tr_tab_qry_rslt1_fig,use_container_width=True)

        df_top_us_tab_qry_rslt1['State'] = df_top_us_tab_qry_rslt1['State'].astype(str)
        df_top_us_tab_qry_rslt1['Total User count'] = df_top_us_tab_qry_rslt1['Total User count'].astype(float)
        df_top_us_tab_qry_rslt1_fig = px.bar(df_top_us_tab_qry_rslt1 , x = 'State', y ='Total User count', color ='Total User count', color_continuous_scale = 'thermal', title = 'Top 10 User Chart', height = 600,)
        df_top_us_tab_qry_rslt1_fig.update_layout(title_font=dict(size=34),title_font_color='#6739b7',paper_bgcolor="rgba(0, 0, 0, 0)")
        df_top_us_tab_qry_rslt1_fig.update_layout(xaxis=dict(tickfont=dict(color='black'),title_font=dict(color='black')),yaxis=dict(tickfont=dict(color='black')))
        st.plotly_chart(df_top_us_tab_qry_rslt1_fig,use_container_width=True)

        st.header(':violet[Total calculation]')
        col3, col4 = st.columns(2)
        with col3:
            st.subheader('Top Transaction Analysis')
            st.dataframe(df_top_tr_tab_qry_rslt1)
        with col4:
            st.subheader('Total User Analysis')
            st.dataframe(df_top_us_tab_qry_rslt1)

