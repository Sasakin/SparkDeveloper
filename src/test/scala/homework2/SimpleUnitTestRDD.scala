package homework2

import homework2.TaxiTimeRDD.{processTaxiData, readRDD}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class SimpleUnitTestRDD extends AnyFlatSpec {


    implicit val spark = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Test №1 for Big Data Application")
      .getOrCreate()

    it should "upload and process data" in {
      val taxiRDD = readRDD("src/main/resources/data/yellow_taxi_jan_25_2018")

      val actualDistribution = processTaxiData(taxiRDD).first()

      assert(actualDistribution._1 == "20")
      assert(actualDistribution._2 == 22044)

    }


  }
