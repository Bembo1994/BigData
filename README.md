# BigData_primo_progetto


# Specifiche 

Le specifiche di questo progetto sono visibili nel file Specifiche Primo Progetto.pdf

# Relazione

La relazione di questo progetto è visibile nel file RelazioneBigData.pdf

# Struttura 

Vi è una cartella "dataset" dove vi sono i dataframe originali, ripuliti e diverse versioni di historical stock prices.csv al crescere della sua dimensione (in milioni di righe). Vi è una cartella per ogni job, dove al suo interno vi sono le tre implementazioni (MapReduce, Hive, Spark) e l'output ottenuto.

# Pulizia Dataset

Con il notebook Data_Cleaning.ipynb si è eseguita una pulizia del dataset in particolare di "historical stocks.csv", salvando la sua versione pulita nominata "clean hs.csv".

# MapReduce

Tali implementazioni sono state eseguite su Hadoop 2.10.1. Si è cercato di ottimizzare il codice relativo all'implementazione di MapReduce differenziandolo dalla modalità cluster e standalone. Infatti per il primo e secondo job si può notare che per la modalità standalone vi sono un solo mapper ed un solo reducer. Dopo aver caricato i file di input su hdfs, per eseguire in modalità standalone è stato utilizzato il comando " $HADOOP_HOME/bin/hadoop jar pathJarFile -mapper pathMapper -reducer pathReducer -input pathInputHDFS -output pathOutputHDFS ". Per la modalità cluster invece è stato usato lo stesso comando ma aggiungendo i parametri "-file" prima dei parametri "-mapper" e "-reducer", con i rispettivi path.

# Hive

Si è utilizzata la versione 2.3.7 di Hive.

# Spark 

Si è utilizzata la versione 3.1.1 di Spark. Per eseguire le implementazione relative a Spark in modalità standalone è stato utilizzato il comando " $SPARK_HOME/bin/spark-submit --master local[*] sparkFile.py " e in sparkFile.py il percorso di input ed output dovrà iniziare con "file://". Se invece le si vuole eseguire in modalità cluster, bisognerà innanzitutto caricare i file di input su hdfs, cambiare il percorso di input ed output in sparkFile.py sostituendo "file://" con "hdfs://" e utilizzando lo stesso comando ma usando "yarn" come valore del parametro "--master".
