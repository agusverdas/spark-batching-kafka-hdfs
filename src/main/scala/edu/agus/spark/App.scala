package edu.agus.spark

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("app-name").getOrCreate()
    val df = spark.read.format("com.databricks.spark.avro").load("hdfs://sandbox-hdp:8020/user/hadoop/expedia")
    println("Expedia dataset was read.")
    df.printSchema()
    println("Expedia data sample")
    df.show(10)

    import spark.implicits._

    val kafkaDF = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp:6667")
      .option("subscribe", "day_weather_hotel")
      .option("startingOffsets", "earliest")
      .load()
    val topicDF = kafkaDF.selectExpr("CAST(value AS STRING)")
    println("Kafka dataset was read")
    topicDF.printSchema()
    println("Kafka data sample")
    topicDF.show(10)

    val schema = spark.read.json(topicDF.select("value").as[String]).schema
    val valueDF = topicDF.select(from_json(col("value"), schema).as("s")).select("s.*")
    // Info about hotels from Kafka
    val hotelInfo = valueDF.select(col("Id"), col("Name"), col("Address"), col("City"),
      col("Country"), col("Latitude"), col("Longitude")).distinct()

    val window = Window.partitionBy("hotel_id").orderBy("srch_ci", "srch_co")
    val windowedDF = df.withColumn("prev_co",
      // Default value for 'prev_co' is 'srch_ci'
      coalesce(lag("srch_co", 1) over window, col("srch_ci")))
        .withColumn("idle", when(datediff(col("srch_ci"),col("prev_co")) > 0,
          datediff(col("srch_ci"), col("prev_co"))) otherwise 0)

    // Ids of "invalid" hotels
    val filterdIds = windowedDF.select("hotel_id")
        .where(col("idle") >= 2 && col("idle") < 30)
        .distinct()

    val invalidHotels = hotelInfo.join(filterdIds, hotelInfo("Id") === filterdIds("hotel_id"), "inner")
    println("Invalid hotels info")
    invalidHotels.show(25, truncate = false)

    //LEFT ANTI JOIN to retrieve only valid orders
    val validOrders = windowedDF.join(filterdIds, windowedDF("hotel_id") === filterdIds("hotel_id"),"leftanti")

    // here are valid orders with hotel info(Name, Address, City, Country, Lat., Long.)
    val valid = validOrders.join(hotelInfo, validOrders("hotel_id") === hotelInfo("Id"))
        .drop(hotelInfo("Id"))
    println("Valid hotels DF")
    valid.show(25, truncate = false)

    println("Valid data count booking group by country : ")
    valid.groupBy("Country").agg(count("id")).show(25, false)

    println("Valid data count booking group by city : ")
    valid.groupBy("City").agg(count("id")).show(25, false)

    val partitionDF = valid.withColumn("year", year(col("srch_ci")))

    partitionDF
      .write
      .partitionBy("year")
      .mode(SaveMode.Overwrite)
      .parquet("hdfs://sandbox-hdp:8020/user/hadoop/modify")
  }
}
