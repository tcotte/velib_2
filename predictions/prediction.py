import warnings
import itertools
import numpy as np
from dataProcessing import get_stations, sorted_positions, sorted_capacities, full_station
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

warnings.filterwarnings("ignore")
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.api import VAR  # vector auto reg
from statsmodels.iolib.smpickle import load_pickle
import time
import dataProcessing as dp  # for get_capacities, get stations

y_df = pd.read_csv("dataframe_streaming.csv")  # Velib/ #??model_fit = VAR.load('ar_model.pkl')

model_fit = load_pickle('../model/ar_model.pkl')


def predict_nextstate(y_df, model_fit):
    df_s_c = pd.read_csv("../training_csv/stations_capacities.csv")  # Velib/ TODO once in github
    stations = dp.get_stations()
    capacities = dp.get_capacities(df_s_c)

    # remove dates
    y = y_df.drop(columns=['Date']).to_numpy()

    # Buid a vector .. [Y]
    # nombre de temps enregistrés
    times = len(y) / len(stations)
    print("nombre de temps dans les données de test: ", times)
    # we need to re-order with the order of training: stations
    y_series = [[] for t in range(int(times))]
    by_station = []
    for s in stations:
        station_s = y_df['Available_bikes'].loc[y_df['Station'] == s]
        station_s = station_s.tolist()
        by_station.append(station_s)
    for t in range(int(times)):
        serie = []
        for s in range(len(stations)):
            tata = by_station[s][t]
            serie.append(tata)
        y_series.append(np.array(serie))

    y_series = np.array(y_series)
    # get lastobs #probleme de mois?
    last_obs_df = y_series[-1]

    # Make a one-step prediction
    data = y_series
    last_ob = last_obs_df

    # make prediction VARresults
    predictions = model_fit.forecast(data, 1)  # 1 step, 30 min

    # transform prediction
    yhat = predictions[
               0] + last_ob  # prediction ne donne qu'une évolution, on doit l'ajouter à la dernière valeur connue

    # Post-processing: pas plus de véo que capacity et pas moins que 0

    # arrondir
    yhat = np.round(yhat)
    for i in range(len(yhat)):
        yhat[i] = int(yhat[i])
        if yhat[i] < 0:
            yhat[i] = 0
        if yhat[i] > capacities[i]:
            yhat[i] = capacities[i]

    return yhat


sc = SparkContext(appName="PythonSparkStreamingPredictions")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 2)  # Creating Kafka direct stream

while True:
    yhat = [int(i) for i in predict_nextstate(y_df, model_fit)]
    pairs_predicted = [[int(i) for i in get_stations()], yhat]

    station = [int(i) for i in get_stations()]
    capacity = sorted_capacities(get_stations())

    data = list(zip(station, yhat, sorted_positions(get_stations()), capacity, full_station(yhat, capacity)))
    df = spark.createDataFrame(data, ["Station", "Available_bikes", "Position", "Capacity", "Full"])

    # columns = ["Station", "Available_bikes"]

    # df = spark.createDataFrame(pairs_predicted, columns)
    df.show()
    df.write.format(
        'org.elasticsearch.spark.sql'
    ).mode(
        'overwrite'  # or .mode('append')
    ).option(
        'es.nodes', 'localhost'
    ).option(
        'es.port', 9200
    ).option(
        'es.resource', '%s/%s' % ('velotoulouse-predictions', '_doc'),
    ).save()

    time.sleep(30 * 60)
