# Velib

### Notes pour le CSV

Pour lancer le projet et lire le CSV, commence par supprimer ce qu'il y a dans le csv que tu téléchargeras (ce qui est là est à titre d'exemple). Ensuite, lance zookeeper 
et kafka en créant d'abord un topic "**velib**";
Puis lance le producer (**call_api.py**), il permet de récupérer ce qu'il ya dans l'API toutes les minutes. Ensuite lance le fichier **consumer.py** qui récupérera les données
intéressantes et les stockera dans le fichier **velib.csv**.

### Notes pour SparkStreaming 

Lancer spark-submit avec la commande ->  spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 read_kafka.py  
