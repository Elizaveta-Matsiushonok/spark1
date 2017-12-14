package com.epam.hubd.spark.scala.core.homework

import java.io.File
import java.nio.file.Files

import com.epam.hubd.spark.scala.core.homework.MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR}
import com.epam.hubd.spark.scala.core.homework.domain.{BidItem, EnrichedItem}
import com.epam.hubd.spark.scala.core.util.RddComparator
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext, SparkContextProvider}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

/**
  * Created by Csaba_Bejan on 8/17/2016.
  */
class MotelsHomeRecommendationTest extends FunSuite with SharedSparkContext with RDDComparisons with BeforeAndAfter
  with BeforeAndAfterAll with SparkContextProvider {

  override def conf = new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test")

  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"
  val INPUT_EXCHANGE_RATES_SAMPLE = "src/test/resources/exchange_rate_sample.txt"
  val INPUT_MOTELS_SAMPLE = "src/test/resources/motels_sample.txt"

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.txt"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.txt"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/error_records"

  private var outputFolder: File = null

  before {
    System.setProperty("hadoop.home.dir", System.getProperty("user.dir"))
    sys.props.foreach(println)
    outputFolder = Files.createTempDirectory("output").toFile
  }

  test("should read raw bids") {
    val expected = sc.parallelize(
      Seq(
        List("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL"),
        List("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35"),
        List("0000006", "08-05-08-2016", "1.35", "", "1.13", "2.02", "1.33", "", "1.64", "1.70", "0.45", "2.02", "1.87", "1.75", "0.45", "1.28", "1.15"),
        List("0000003", "07-05-08-2016", "0.80", "1.05", "1.49", "0.43", "1.63", "0.42",
          "", "1.44", "1.73", "1.99", "1.86", "1.99", "0.97", "0.93", "", "0.51")
      )
    )

    val rawBids = MotelsHomeRecommendation.getRawBids(sc, INPUT_BIDS_SAMPLE)

    assertRDDEquals(expected, rawBids)
  }

  test("should collect erroneous records") {
    val rawBids = sc.parallelize(
      Seq(
        List("1", "06-05-02-2016", "ERROR_1"),
        List("2", "15-04-08-2016", "0.89"),
        List("3", "07-05-02-2016", "ERROR_2"),
        List("4", "06-05-02-2016", "ERROR_1"),
        List("5", "06-05-02-2016", "ERROR_2")
      )
    )

    val expected = sc.parallelize(
      Seq(
        "06-05-02-2016,ERROR_1,2",
        "06-05-02-2016,ERROR_2,1",
        "07-05-02-2016,ERROR_2,1"
      )
    )

    val erroneousRecords = MotelsHomeRecommendation.getErroneousRecords(rawBids)

    assertRDDEquals(expected, erroneousRecords)
  }

  test("should read the exchange rate information") {
    val expected = Map(
      "11-06-05-2016" -> 0.803,
      "11-05-08-2016" -> 0.873,
      "10-06-11-2015" -> 0.987
    )

    val exchangeRates = MotelsHomeRecommendation.getExchangeRates(sc, INPUT_EXCHANGE_RATES_SAMPLE)

    Assert.assertEquals(expected, exchangeRates)
  }

  test("should read conforming bids, keeping three countries, convert USD to EUR and dates to proper format, get rid of records where is no price for a Losa") {

    val expected = sc.parallelize(
      Seq(
        BidItem("0000006", "2016-08-05 08:00", "US", 1.765),
        BidItem("0000006", "2016-08-05 08:00", "CA", 1.433),
        BidItem("0000006", "2016-08-05 08:00", "MX", 1.162),
        BidItem("0000003", "2016-08-05 07:00", "US", 0.345),
        BidItem("0000003", "2016-08-05 07:00", "CA", 1.307)
      )
    )

    val exchangeRates = MotelsHomeRecommendation.getExchangeRates(sc, INPUT_EXCHANGE_RATES_INTEGRATION)
    val rawBids = MotelsHomeRecommendation.getRawBids(sc, INPUT_BIDS_SAMPLE)

    val bids = MotelsHomeRecommendation.getBids(rawBids, exchangeRates)
    assertRDDEquals(expected, bids)

  }

  test("should read motels") {

    val expected = sc.parallelize(
      Seq(
        ("0000001", "Olinda Windsor Inn"),
        ("0000002", "Merlin Por Motel"),
        ("0000003", "Olinda Big River Casino")
      )
    )


    val motels = MotelsHomeRecommendation.getMotels(sc, INPUT_MOTELS_SAMPLE)
    assertRDDEquals(expected, motels)
  }

  test("should create enrichedBids") {

    val expected = sc.parallelize(
      Seq(
        EnrichedItem("0000006", "Mengo Elegance River Side Hotel", "2016-08-05 08:00", "US", 1.765),
        EnrichedItem("0000003", "Olinda Big River Casino", "2016-08-05 07:00", "CA", 1.307)

      )
    )

    val rawBids = MotelsHomeRecommendation.getRawBids(sc, INPUT_BIDS_SAMPLE)
    val exchangeRates = MotelsHomeRecommendation.getExchangeRates(sc, INPUT_EXCHANGE_RATES_INTEGRATION)
    val bids = MotelsHomeRecommendation.getBids(rawBids, exchangeRates)
    val motels = MotelsHomeRecommendation.getMotels(sc, INPUT_MOTELS_INTEGRATION)

    val enrichedBids = MotelsHomeRecommendation.getEnriched(bids, motels)
    assertRDDEquals(expected, enrichedBids)
  }

  test("should filter errors and create correct aggregates") {

    runIntegrationTest()

    //If the test fails and you are interested in what are the differences in the RDDs uncomment the corresponding line
    //printRddDifferences(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    // printRddDifferences(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertAggregatedFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  after {
    outputFolder.delete
  }

  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(sc, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    assertRDDEquals(expected, actual)
  }

  private def assertAggregatedFiles(expectedPath: String, actualPath: String) = {
    val expected = parseLastDouble(sc.textFile(expectedPath)).collect.toMap
    val actual = parseLastDouble(sc.textFile(actualPath)).collect.toMap
    if (expected.size != actual.size) {
      Assert.fail(s"Aggregated have wrong number of records (${actual.size} instead of ${expected.size})")
    }
    expected.foreach(x => {
      val key = x._1
      val expectedValue = x._2
      if (!actual.contains(key)) {
        Assert.fail(s"Aggregated does not contain: $key,$expectedValue")
      }
      val actualValue = actual(key)
      if (Math.abs(expectedValue - actualValue) > 0.0011) {
        Assert.fail(s"Aggregated have different value for: $key ($actualValue instead of $expectedValue)")
      }
    })
  }

  private def parseLastDouble(rdd: RDD[String]) = {
    rdd.map(s => {
      val commaIndex = s.lastIndexOf(",")
      (s.substring(0, commaIndex), s.substring(commaIndex + 1).toDouble)
    })
  }

  private def printRddDifferences(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}
