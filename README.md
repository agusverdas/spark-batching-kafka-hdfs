# Big Data Training (Spark Batching)
Для сборки:

    mvn clean install

Для запуска(HDP 3.0.1):

    spark-submit \ 
    --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 \
    --class edu.agus.spark.App \
    --master yarn-client \
    --num-executors {number_executors} \
    --driver-memory {driver_memory} \
    --executor-memory {executor_memory} \
    --executor-cores {number_of_cores} \
    scala-spark-batch-maven-1.0-SNAPSHOT-jar-with-dependencies.jar