package homework2


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, explode}

import java.util.Properties

object DataApiHomeWorkTaxi extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"

  //1
  val taxiFactsDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiFactsDF.printSchema()
  println(taxiFactsDF.count())

  val taxiZoneDF = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/data/taxi_zones.csv")

  taxiZoneDF.printSchema()
  println(taxiZoneDF.count())

  val df = taxiFactsDF
    .join(broadcast(taxiZoneDF), col("DOLocationID") === col("LocationID"), "left")
    .groupBy(col("Borough"))
    .count()
    .orderBy(col("count").desc)

  df.show()
  df.write.parquet("src/main/resources/data/brough.parquet")



  //2

  case class TaxiTime(tpep_pickup_datetime: String,
                      hh_pickup: String)

  val context = spark.sparkContext

  val taxiTimeRDD = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018").rdd

  val hhCallTable = taxiTimeRDD.map(t => TaxiTime(String.valueOf(t(1)), String.valueOf(t(2)).split(" ")(1).split(":")(0)))
    .map(tt => (tt.hh_pickup, 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, false)

  hhCallTable.foreach(x => println(x))

  hhCallTable.map(t => new String(t._1 + " " + t._2)).saveAsTextFile("src/main/resources/data/hhCallTable")

  // 3

  case class TaxiDistance( VendorID: Int,
                           trip_distance: Double)

  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")

  import spark.implicits._

  val taxiDistanceDS = taxiDF.as[TaxiDistance]

  val distributionDS = taxiDistanceDS.map(x => new TaxiDistance(x.VendorID, x.trip_distance.round))
    .groupBy(col("trip_distance"))
    .count()
    .orderBy(col("count").desc)

  distributionDS.show()

  val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)

  distributionDS.write.option("driver", driver).jdbc(url, "distance_distribution", properties)

}

