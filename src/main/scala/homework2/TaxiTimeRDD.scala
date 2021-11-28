package homework2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object TaxiTimeRDD {

  case class TaxiTime(tpep_pickup_datetime: String,
                      hh_pickup: String)

  def readRDD(path: String)(implicit spark: SparkSession): RDD[Row] = spark.read.load(path).rdd

  def processTaxiData(taxiRDD: RDD[Row]) = {
    taxiRDD.map(t => TaxiTime(String.valueOf(t(1)), String.valueOf(t(2)).split(" ")(1).split(":")(0)))
      .map(tt => (tt.hh_pickup, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
  }

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Taxi Big Data Application")
      .getOrCreate()


    val taxiRDD = readRDD("src/main/resources/data/yellow_taxi_jan_25_2018")

    val value = processTaxiData(taxiRDD)
    value.foreach(x => println(x))

    //value.map(t => new String(t._1 + " " + t._2)).saveAsTextFile("src/main/resources/data/hhCallTable")
  }
}
