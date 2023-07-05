import requests
import subprocess
import pandas as pd
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import os
import json

# "C:\Users\Mouli\Desktop\New folder"
response = requests.get('https://api.github.com/repos/PhonePe/pulse')
repo = response.json()
clone_url = repo['clone_url']
clone_dir = "C:/Users/Mouli/Desktop/guvi/phonepe"
subprocess.run(["git", "clone", clone_url, clone_dir], check=True)

path_1 = "C:/Users/Mouli/Desktop/guvi/phonepe/data/aggregated/transaction/country/india/state/"
Agg_tran_state_list = os.listdir(path_1)

Agg_tra = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}

for i in Agg_tran_state_list:
    p_i = path_1 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            A = json.load(Data)
            
            for l in A['data']['transactionData']:
                Name = l['name']
                count = l['paymentInstruments'][0]['count']
                amount = l['paymentInstruments'][0]['amount']
                Agg_tra['State'].append(i)
                Agg_tra['Year'].append(j)
                Agg_tra['Quarter'].append(int(k.strip('.json')))
                Agg_tra['Transaction_type'].append(Name)
                Agg_tra['Transaction_count'].append(count)
                Agg_tra['Transaction_amount'].append(amount)
                
df_aggregated_transaction = pd.DataFrame(Agg_tra)

path_2 = "C:/Users/Mouli/Desktop/guvi/phonepe/data/aggregated/user/country/india/state/"
Agg_user_state_list = os.listdir(path_2)

Agg_user = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'User_Count': [], 'User_Percentage': []}

for i in Agg_user_state_list:
    p_i = path_2 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            B = json.load(Data)
            
            try:
                for l in B["data"]["usersByDevice"]:
                    brand_name = l["brand"]
                    count_ = l["count"]
                    ALL_percentage = l["percentage"]
                    Agg_user["State"].append(i)
                    Agg_user["Year"].append(j)
                    Agg_user["Quarter"].append(int(k.strip('.json')))
                    Agg_user["Brands"].append(brand_name)
                    Agg_user["User_Count"].append(count_)
                    Agg_user["User_Percentage"].append(ALL_percentage*100)
            except:
                pass

df_aggregated_user = pd.DataFrame(Agg_user)

path_3 = "C:/Users/Mouli/Desktop/guvi/phonepe/data/map/transaction/hover/country/india/state/"
map_tra_state_list = os.listdir(path_3)

map_tra = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Transaction_Count': [], 'Transaction_Amount': []}

for i in map_tra_state_list:
    p_i = path_3 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            C = json.load(Data)
            
            for l in C["data"]["hoverDataList"]:
                District = l["name"]
                count = l["metric"][0]["count"]
                amount = l["metric"][0]["amount"]
                map_tra['State'].append(i)
                map_tra['Year'].append(j)
                map_tra['Quarter'].append(int(k.strip('.json')))
                map_tra["District"].append(District)
                map_tra["Transaction_Count"].append(count)
                map_tra["Transaction_Amount"].append(amount)
                
df_map_transaction = pd.DataFrame(map_tra)

path_4 = "C:/Users/Mouli/Desktop/guvi/phonepe/data/map/user/hover/country/india/state/"
map_user_state_list = os.listdir(path_4)

map_user = {"State": [], "Year": [], "Quarter": [], "District": [], "Registered_User": []}

for i in map_user_state_list:
    p_i = path_4 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            D = json.load(Data)

            for l in D["data"]["hoverData"].items():
                district = l[0]
                registereduser = l[1]["registeredUsers"]
                map_user['State'].append(i)
                map_user['Year'].append(j)
                map_user['Quarter'].append(int(k.strip('.json')))
                map_user["District"].append(district)
                map_user["Registered_User"].append(registereduser)
                
df_map_user = pd.DataFrame(map_user)

path_5 = "C:/Users/Mouli/Desktop/guvi/phonepe/data/top/transaction/country/india/state/"
top_tra_state_list = os.listdir(path_5)

top_tra = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Transaction_count': [], 'Transaction_amount': []}

for i in top_tra_state_list:
    p_i = path_5 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            E = json.load(Data)
            
            for l in E['data']['pincodes']:
                Name = l['entityName']
                count = l['metric']['count']
                amount = l['metric']['amount']
                top_tra['State'].append(i)
                top_tra['Year'].append(j)
                top_tra['Quarter'].append(int(k.strip('.json')))
                top_tra['District_Pincode'].append(Name)
                top_tra['Transaction_count'].append(count)
                top_tra['Transaction_amount'].append(amount)

df_top_transaction = pd.DataFrame(top_tra)


path_6 = "C:/Users/Mouli/Desktop/guvi/phonepe/data/top/user/country/india/state/"
top_user_state_list = os.listdir(path_6)

top_user = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Registered_User': []}

for i in top_user_state_list:
    p_i = path_6 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            F = json.load(Data)
            
            for l in F['data']['pincodes']:
                Name = l['name']
                registeredUser = l['registeredUsers']
                top_user['State'].append(i)
                top_user['Year'].append(j)
                top_user['Quarter'].append(int(k.strip('.json')))
                top_user['District_Pincode'].append(Name)
                top_user['Registered_User'].append(registeredUser)
                
df_top_user = pd.DataFrame(top_user)

mydb = mysql.connector.connect(
  host = "localhost",
  user = "root",
  password = "1234",
  auth_plugin = "mysql_native_password"
)

mycursor = mydb.cursor()
mycursor.execute("CREATE DATABASE IF NOT EXISTS phonepe_pulse")

mycursor.close()
mydb.close()

engine = create_engine('mysql+mysqlconnector://root:1234@localhost/phonepe_pulse', echo=False)

df_aggregated_transaction.to_sql('aggregated_transaction', engine, if_exists = 'replace', index=False,   
                                 dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                       'Year': sqlalchemy.types.Integer, 
                                       'Quater': sqlalchemy.types.Integer, 
                                       'Transaction_type': sqlalchemy.types.VARCHAR(length=50), 
                                       'Transaction_count': sqlalchemy.types.Integer,
                                       'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

df_aggregated_user.to_sql('aggregated_user', engine, if_exists = 'replace', index=False,
                          dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                 'Year': sqlalchemy.types.Integer, 
                                 'Quater': sqlalchemy.types.Integer,
                                 'Brands': sqlalchemy.types.VARCHAR(length=50), 
                                 'User_Count': sqlalchemy.types.Integer, 
                                 'User_Percentage': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
                     
df_map_transaction.to_sql('map_transaction', engine, if_exists = 'replace', index=False,
                          dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                 'Year': sqlalchemy.types.Integer, 
                                 'Quater': sqlalchemy.types.Integer, 
                                 'District': sqlalchemy.types.VARCHAR(length=50), 
                                 'Transaction_Count': sqlalchemy.types.Integer, 
                                 'Transaction_Amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

df_map_user.to_sql('map_user', engine, if_exists = 'replace', index=False,
                   dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                          'Year': sqlalchemy.types.Integer, 
                          'Quater': sqlalchemy.types.Integer, 
                          'District': sqlalchemy.types.VARCHAR(length=50), 
                          'Registered_User': sqlalchemy.types.Integer, })
            
df_top_transaction.to_sql('top_transaction', engine, if_exists = 'replace', index=False,
                         dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                'Year': sqlalchemy.types.Integer, 
                                'Quater': sqlalchemy.types.Integer,   
                                'District_Pincode': sqlalchemy.types.Integer,
                                'Transaction_count': sqlalchemy.types.Integer, 
                                'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

df_top_user.to_sql('top_user', engine, if_exists = 'replace', index=False,
                   dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                          'Year': sqlalchemy.types.Integer, 
                          'Quater': sqlalchemy.types.Integer,                           
                          'District_Pincode': sqlalchemy.types.Integer, 
                          'Registered_User': sqlalchemy.types.Integer,})

# from sqlalchemy import create_engine
# import sqlalchemy.types

# # Connect to the SQLite database
# conn = sqlite3.connect('phonepe_pulse.db')

# # Create a cursor object
# cursor = conn.cursor()

# # Create a new table for aggregated_transaction
# cursor.execute('''CREATE TABLE IF NOT EXISTS aggregated_transaction (
#                     State TEXT,
#                     Year INTEGER,
#                     Quater INTEGER,
#                     Transaction_type TEXT,
#                     Transaction_count INTEGER,
#                     Transaction_amount REAL
#                   )''')

# # Create a new table for aggregated_user
# cursor.execute('''CREATE TABLE IF NOT EXISTS aggregated_user (
#                     State TEXT,
#                     Year INTEGER,
#                     Quater INTEGER,
#                     Brands TEXT,
#                     User_Count INTEGER,
#                     User_Percentage REAL
#                   )''')

# # Create a new table for map_transaction
# cursor.execute('''CREATE TABLE IF NOT EXISTS map_transaction (
#                     State TEXT,
#                     Year INTEGER,
#                     Quater INTEGER,
#                     District TEXT,
#                     Transaction_Count INTEGER,
#                     Transaction_Amount REAL
#                   )''')

# # Create a new table for map_user
# cursor.execute('''CREATE TABLE IF NOT EXISTS map_user (
#                     State TEXT,
#                     Year INTEGER,
#                     Quater INTEGER,
#                     District TEXT,
#                     Registered_User INTEGER
#                   )''')

# # Create a new table for top_transaction
# cursor.execute('''CREATE TABLE IF NOT EXISTS top_transaction (
#                     State TEXT,
#                     Year INTEGER,
#                     Quater INTEGER,
#                     District_Pincode INTEGER,
#                     Transaction_count INTEGER,
#                     Transaction_amount REAL
#                   )''')

# # Create a new table for top_user
# cursor.execute('''CREATE TABLE IF NOT EXISTS top_user (
#                     State TEXT,
#                     Year INTEGER,
#                     Quater INTEGER,
#                     District_Pincode INTEGER,
#                     Registered_User INTEGER
#                   )''')

# # Close the cursor and commit the changes to the database
# cursor.close()
# conn.commit()

# # Connect to the new created database using SQLAlchemy
# engine = create_engine('sqlite:///phonepe_pulse.db', echo=False)

# # Use pandas to insert the DataFrames data to the SQL Database -> table1

# # 1
# df_aggregated_transaction.to_sql('aggregated_transaction', engine, if_exists='replace', index=False,
#                                  dtype={'State': sqlalchemy.types.TEXT,
#                                         'Year': sqlalchemy.types.INTEGER,
#                                         'Quater': sqlalchemy.types.INTEGER,
#                                         'Transaction_type': sqlalchemy.types.TEXT,
#                                         'Transaction_count': sqlalchemy.types.INTEGER,
#                                         'Transaction_amount': sqlalchemy.types.REAL})

# # 2
# df_aggregated_user.to_sql('aggregated_user', engine, if_exists='replace', index=False,
#                           dtype={'State': sqlalchemy.types.TEXT,
#                                  'Year': sqlalchemy.types.INTEGER,
#                                  'Quater': sqlalchemy.types.INTEGER,
#                                  'Brands': sqlalchemy.types.TEXT,
#                                  'User_Count': sqlalchemy.types.INTEGER,
#                                  'User_Percentage': sqlalchemy.types.REAL})

# # 3
# df_map_transaction.to_sql('map_transaction', engine, if_exists='replace', index=False,
#                           dtype={'State': sqlalchemy.types.TEXT,
#                                  'Year': sqlalchemy.types.INTEGER,
#                                  'Quater': sqlalchemy.types.INTEGER,
#                                  'District': sqlalchemy.types.TEXT,
#                                  'Transaction_Count': sqlalchemy.types.INTEGER,
#                                  'Transaction_Amount': sqlalchemy.types.REAL})

# # 4
# df_map_user.to_sql('map_user', engine, if_exists='replace', index=False,
#                    dtype={'State': sqlalchemy.types.TEXT,
#                           'Year': sqlalchemy.types.INTEGER,
#                           'Quater': sqlalchemy.types.INTEGER,
#                           'District': sqlalchemy.types.TEXT,
#                           'Registered_User': sqlalchemy.types.INTEGER})

# # 5
# df_top_transaction.to_sql('top_transaction', engine, if_exists='replace', index=False,
#                           dtype={'State': sqlalchemy.types.TEXT,
#                                  'Year': sqlalchemy.types.INTEGER,
#                                  'Quater': sqlalchemy.types.INTEGER,
#                                  'District_Pincode': sqlalchemy.types.INTEGER,
#                                  'Transaction_count': sqlalchemy.types.INTEGER,
#                                  'Transaction_amount': sqlalchemy.types.REAL})

# # 6
# df_top_user.to_sql('top_user', engine, if_exists='replace', index=False,
#                    dtype={'State': sqlalchemy.types.TEXT,
#                           'Year': sqlalchemy.types.INTEGER,
#                           'Quater': sqlalchemy.types.INTEGER,
#                           'District_Pincode': sqlalchemy.types.INTEGER,
#                           'Registered_User': sqlalchemy.types.INTEGER})

# # Close the connection to the SQLite database
# conn.close()
