# VélôToulouse

### Objectifs
Le premier jectif de ce projet était de représenter, en temps réel, sur une carte, l'état des stations VélôToulouse en temps réel.
Le second objectif était de retranscrire sur une carte l'état des stations dans 30 minutes grâce à un model "time-series" crée
grâce à la librairie Python *statsmodel*.

<p align="center">
  <img src="images/VeloToulouse.PNG" width="350" title="hover text">
</p>

### Logiciels requis

Pour pouvoir utiliser ce projet, les installations de quelques logiciels sont nécessaires :
- Kafka
- Spark
- Hadoop
- Kibana X.X.X
- ElasticSearch X.X.X
- elasticsearch-hadoop-X.X.X.jar

Les "X" sont les numéros de version, ils doivent être similaires pour Kibana, ElasticSearch et le jar complémentaire qui permettra
d'envoyer nos données depuis Spark dans Elastic Search.

### Configuration des variables d'environnement

Après avoir installé les logiciels, il faut configurer quelques variables d'environnement dans le *~/.bashrc* :
- SPARK_HOME
- HADOOP_HOME
- PYTHONPATH
- PYSPARK_PYTHON

Une fois que ces variables sont correctement configurées, il faut installer les dépendances Python du projet. Pour cela,
il est fortement conseillé d'utiliser un environnement virtuel Python.
Vous pouvez installer automatiquement toutes les dépedances à l'aide de la commande :
```
pip install -r requirements.txt
```


Maintenant que les installations sont réalisées nous allons poursuivre en expliquant brièvement le projet.

### Architecture du projet

<p align="center">
  <img src="images/graph_application.PNG" width="550" title="hover text">
</p>



### Notes pour le CSV

Pour lancer le projet et lire le CSV, commence par supprimer ce qu'il y a dans le csv que tu téléchargeras (ce qui est là est à titre d'exemple). Ensuite, lance zookeeper 
et kafka en créant d'abord un topic "**velib**";
Puis lance le producer (**call_api.py**), il permet de récupérer ce qu'il ya dans l'API toutes les minutes. Ensuite lance le fichier **consumer.py** qui récupérera les données
intéressantes et les stockera dans le fichier **velib.csv**.

### Notes pour SparkStreaming 

Lancer spark-submit avec la commande ->  spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 read_kafka.py  


## Elastic search
**csv file import**
https://techexpert.tips/fr/elasticsearch-fr/elasticsearch-importation-dun-fichier-csv/

---
### Launch zookeeper
```
./bin/zookeeper-server-start.sh ./config/zookeeper.properties
```

### Launch Kafka
```
./bin/kafka-server-start.sh ./config/server.properties
```

### Call API 
```
python3 call_apy.py
```

### Consumer
```
python3 producer_predict.py
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 kafka_to_df.py
```

### Kafka to es
```
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 --jars ../elasticsearch-hadoop-7.10.2.jar --driver-class-path ../elasticsearch-hadoop-7.10.2.j
ar kafka_to_es.py
```

### Prediction
```
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 --jars ../elasticsearch-hadoop-7.10.2.jar --driver-class-path ../elasticsearch-hadoop-7.10.2.j
ar prediction.py
```





