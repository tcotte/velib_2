import warnings
import itertools
import numpy as np

# import matplotlib.pyplot as plt
warnings.filterwarnings("ignore")
import pandas as pd
import statsmodels.api as sm
# from statsmodels.tsa.ar_model import AutoReg
from statsmodels.tsa.api import VAR  # vector auto reg

import dataProcessing as dp  # for get_capacities, get stations

# PARAMETERS
max_lags = 10

new_df = pd.read_csv("../Velib/filtered_velib_clean.csv")

df_s_c = pd.read_csv("../training_csv/stations_capacities.csv")  # Velib/ TODO once in github
stations = dp.get_stations(df_s_c)
capacities = dp.get_capacities(df_s_c)

# lists par station
y_list = []
for s in range(len(stations)):
    analyse = new_df.loc[new_df['station_id'] == stations[s]]
    analyse = analyse.drop(columns=['station_id'])
    analyse = analyse.set_index('date')
    y = analyse['available bikes']
    y = y.reset_index()
    y_list.append(y)


def plot_bikes(y_list, s):
    y_list[s].plot(figsize=(15, 6))
    plt.show()
    return


# Buid a vector .. [X], toutes les stations à un temps t
series = []
for t in range(len(y_list[1])):
    serie = []
    for s in range(len(stations)):
        toto = y_list[s]['available bikes'].iloc[t]
        serie.append(toto)
    series.append(np.array(serie))
series = np.array(series)


# fit an AR model and manually save coefficients to file

# create a difference transform of the dataset
def difference(dataset):
    diff = list()
    for i in range(1, len(dataset)):
        value = dataset[i] - dataset[i - 1]
        diff.append(value)
    return np.array(diff)


# load dataset
X = difference(series)  # series.values
# fit model
model = VAR(X)
# model.select_order(10) #select lag order? 10 dernières valeures, not working
model_fit = model.fit(maxlags=max_lags)

# save model to file
model_fit.save('../model/ar_model.pkl')
# save the differenced dataset
np.save('../ar_data.npy', X)
# save the last ob
# np.save('ar_obs.npy', [series[-1]]) #Useful only to predict step just after the training
print("ok")
