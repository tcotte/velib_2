import warnings
import itertools
import numpy as np
warnings.filterwarnings("ignore")
import pandas as pd
from datetime import timedelta
from datetime import datetime


#Parameters
interval = 30 #interval en minutes

#!git clone https://github.com/tcotte/Velib.git

df1 = pd.read_csv("Velib/velib_clean1.csv")
df2 = pd.read_csv("Velib/velib_clean2.csv")
#print(df1.columns)

#list stations
def get_stations(df):
    stations = np.array(df['station_number'])
    stations = list(dict.fromkeys(stations))
    return stations

def get_capacities(df):
    #total capacity per station
    capacities = []
    for s in stations:
        example = df.loc[df['station_number'] == s].head(1)
        capacity = example['capacity']
        capacities.extend(capacity)
    return capacities
    
def get_start_end(df):    
    start = df['date'].min()
    end = df['date'].max()
    return start, end


df = pd.concat([df1,df2])
#print(df.columns)
df = df.drop(columns = ['Unnamed: 0'])

#remove station Bergerac.. 1033
df = df.loc[df['station_number']!=1033]

df['date'] = pd.to_datetime(df['date'], format = "%m/%d/%Y %H:%M")


stations = get_stations(df)
capacities = get_capacities(df)
start, end = get_start_end(df)

data_s_c = {"station_number":stations , "capacity": capacities}
df_s_c = pd.DataFrame(data=data_s_c)
df_s_c.to_csv("stations_capacities.csv") #? Velib/ or not


#updates per station
updates = []
for s in stations:
    update = df.loc[df['station_number'] == s]
    update["date"] = pd.to_datetime(update['date'], format = "%m/%d/%Y %H:%M")
    updates.append(update)



#Create un DF, une ligne toute les 30min avec nbre de bikes par station
time_list = []
bikes_list = []
s_list = []
for s in range(len(stations)):
    station = stations[s]
    time = start #datetime.strptime(start,"%m/%d/%Y %H:%M" )

    while  (time<end): #datetime.strptime(end,"%m/%d/%Y %H:%M" )
        time_list.append(time)
        #filter df upadtes[s], max < time + 30
        timenew = time + timedelta(minutes = interval)
        inf = updates[s].loc[updates[s]['date'] <= timenew]
        inff = inf['date'].max()
        bikes = inf['available_bikes'].loc[inf['date'] == inff]
        bikes_list.append(bikes.tolist()[0])
        s_list.append(station)
        time = timenew

data = {"station_id":s_list ,"date": time_list, "available bikes": bikes_list}
new_df = pd.DataFrame(data=data)

new_df.to_csv("filtered_velib_clean.csv") #? Velib/ or not
