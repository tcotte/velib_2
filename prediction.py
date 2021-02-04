import warnings
import itertools
import numpy as np
import matplotlib.pyplot as plt
warnings.filterwarnings("ignore")
plt.style.use('fivethirtyeight')
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.api import VAR #vector auto reg
from statsmodels.iolib.smpickle import load_pickle

import dataProcessing as dp # for get_capacities, get stations

y_df = pd.read_csv("Velib/dataframe_streaming_1.csv") #Velib/ #??model_fit = VAR.load('ar_model.pkl')

model_fit = load_pickle('ar_model.pkl')

def predict_nextstate(y_df, model_fit):

    df_s_c = pd.read_csv("stations_capacities.csv") #Velib/ TODO once in github
    stations = dp.get_stations(df_s_c)
    capacities = dp.get_capacities(df_s_c)


    #remove dates
    y = y_df.drop(columns=['Date']).to_numpy()
    #y[i][0] c'est la station et y[i][1] c'est le nombre de bikes

    #Buid a vector .. [Y]
    #nombre de temps enregistrés
    times = len(y)/len(stations)
    print("nombre de temps dans les données de test: ", times)
    #we need to re-order with the order of training: stations
    y_series = [[] for t in range(int(times))]
    by_station = []
    for s in stations:
        station_s = y_df['Available_bikes'].loc[y_df['Station']==s]
        station_s= station_s.tolist()
        by_station.append(station_s)
    for t in range(int(times)):
        serie = []
        for s in range(len(stations)):
            tata = by_station[s][t]
            serie.append(tata)
        y_series.append(np.array(serie))

    y_series = np.array(y_series)
    #get lastobs #probleme de mois?
    last_obs_df = y_series[-1]


    # Make a one-step prediction
    
    data = y_series
    last_ob = last_obs_df

    # make prediction VARresults
    predictions = model_fit.forecast(data, 1) # 1 step, 30 min
    #VARresults.forecast(y, steps[, exog_future])
    # transform prediction
    yhat = predictions[0] + last_ob # prediction ne donne qu'une évolution, on doit l'ajouter à la dernière valeure connue
    #print('evolution: ', predictions[0])
    #print('next step: ', yhat)


    # Post-processing: pas plus de véo que capacity et pas moins que 0

    #arrondir
    yhat = np.round(yhat)
    for i in range(len(yhat)):
        yhat[i] = int(yhat[i])
        if yhat[i] < 0:
            yhat[i]=0
        if yhat[i]>capacities[i]:
            yhat[i]=capacities[i]

    
    return yhat

yhat = predict_nextstate(y_df, model_fit)
print(yhat)