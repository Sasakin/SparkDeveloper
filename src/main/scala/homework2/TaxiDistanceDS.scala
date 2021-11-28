package homework2

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.util.Properties

object TaxiDistanceDS {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"

  case class TaxiDistance( VendorID: Int,
                           trip_distance: Double)

  def readParquet(path: String)(implicit spark: SparkSession): DataFrame = spark.read.load(path)

  def processTaxiData(taxiDF: DataFrame) = {

    import spark.implicits._

    val taxiDistanceDS = taxiDF.as[TaxiDistance]
    taxiDistanceDS.map(x => new TaxiDistance(x.VendorID, x.trip_distance.round))
      .groupBy(col("trip_distance"))
      .count()
      .orderBy(col("count").desc)
  }

  def processWriteOnPostgress(distributionDS: Dataset[Row]): Unit = {
    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)

    distributionDS.write.option("driver", driver).jdbc(url, "distance_distribution", properties)
  }

}
