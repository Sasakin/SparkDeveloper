package homework2

import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TaxiFactsDF {
  def readParquet(path: String)(implicit spark: SparkSession): DataFrame = spark.read.load(path)
  def readCSV(path: String)(implicit spark: SparkSession):DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

  def processTaxiFactsJoin(taxiDF: DataFrame, taxiZoneDF: DataFrame) : DataFrame = {
    taxiDF
      .join(broadcast(taxiZoneDF), col("DOLocationID") === col("LocationID"), "left")
      .groupBy(col("Borough"))
      .count()
      .orderBy(col("count").desc)
  }


  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Taxi Big Data Application")
      .getOrCreate()


    val taxiFactsDF = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val taxiZoneDF = readCSV("src/main/resources/data/taxi_zones.csv")

    val value = processTaxiFactsJoin(taxiFactsDF, taxiZoneDF)
    value.show()

    //value.write.parquet("src/main/resources/data/brough.parquet")
  }

}
