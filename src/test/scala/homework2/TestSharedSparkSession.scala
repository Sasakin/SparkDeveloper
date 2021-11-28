package homework2

import homework2.TaxiFactsDF.processTaxiFactsJoin
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession

class TestSharedSparkSession extends SharedSparkSession {

  test("join - join using") {
    val taxiFactsDF = homework2.TaxiFactsDF.readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val taxiZoneDF = homework2.TaxiFactsDF.readCSV("src/main/resources/data/taxi_zones.csv")

    //val value = processTaxiFactsJoin(taxiFactsDF, taxiZoneDF)

    checkAnswer(
    processTaxiFactsJoin(taxiFactsDF, taxiZoneDF),
      Row("Manhattan", 296527) ::
      Row("Queens", 13819) ::
      Row("Brooklyn", 12672) ::
      Row("Unknown", 6714) ::
      Row("Bronx", 1589) ::
      Row("EWR", 508) ::
      Row("Staten Island", 64) :: Nil)
  }

  test("join - processTaxiData") {
    val taxiDF = TaxiDistanceDS.readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")

    val actualDistribution = TaxiDistanceDS.processTaxiData(taxiDF)

    actualDistribution.show()

    checkAnswer(
      actualDistribution,
      Row(1.0,142786)  ::
        Row(2.0,75289) ::
        Row(3.0,32323) ::
        Row(0.0 ,18019) ::
        Row(4.0 ,15618) ::
        Row(5.0 ,9541) ::
        Row(6.0,6535) ::
        Row(7.0,4729) ::
        Row(9.0,4086) ::
        Row(10.0,3967) ::
        Row(8.0,3819) ::
        Row(11.0,3029) ::
        Row(12.0,2020) ::
        Row(18.0,1771) ::
        Row(17.0,1622) ::
        Row(13.0,1110) ::
        Row(19.0,1078) ::
        Row(16.0,991) ::
        Row(14.0,793) ::
        Row(15.0,788) ::
        Row(20.0,681) ::
        Row(21.0,511) ::
        Row(22.0,262) ::
        Row(23.0,118) ::
        Row(28.0,72) ::
        Row(24.0,64) ::
        Row(27.0,52) ::
        Row(25.0,50) ::
        Row(26.0,47) ::
        Row(29.0,29) ::
        Row(30.0,21) ::
        Row(32.0,10) ::
        Row(37.0,9) ::
        Row(31.0,8) ::
        Row(33.0,7) ::
        Row(41.0,6) ::
        Row(34.0,4) ::
        Row(35.0,4) ::
        Row(40.0,4) ::
        Row(36.0,2) ::
        Row(44.0,2) ::
        Row(52.0,2) ::
        Row(46.0,2) ::
        Row(45.0,2) ::
        Row(66.0,1) ::
        Row(43.0,1) ::
        Row(48.0,1) ::
        Row(47.0,1) ::
        Row(50.0,1) ::
        Row(39.0,1) ::
        Row(42.0,1) ::
        Row(55.0,1) ::
        Row(38.0,1) ::
        Row(54.0,1) :: Nil
    )

  }

}
