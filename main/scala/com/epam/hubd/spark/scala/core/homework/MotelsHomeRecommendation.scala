package com.epam.hubd.spark.scala.core.homework

import com.epam.hubd.spark.scala.core.homework.domain.{BidError, BidItem, EnrichedItem, BidsWithMappedLosas}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  val ERROR_BID_PATTERN = "ERROR(.*)"

  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"

  val INPUT_EXCHANGE_RATE = "src/test/resources/input/exchange_rate.txt"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation"))

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
      */
    val erroneousRecords: RDD[String] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    val enriched: RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  /**
    * This method is used to read bids information from the provided file
    *
    * @param  sc       SparkContext
    * @param  bidsPath path to file from which information should be reading
    * @return RDD with read data
    */

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    val lines = sc.textFile(bidsPath)
    lines.map(line => line.split(Constants.DELIMITER).toList)
  }

  /**
    * This method group all corrupted records by it's type and date.
    *
    * @param  rawBids Rdd with bids data
    * @return RDD with erroneous data
    */

  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = {
    rawBids.filter(_.exists(record => record.matches(ERROR_BID_PATTERN)))
      .map(record => (BidError(record(1), record(2)), 1)).reduceByKey(_ + _)
      .map(record => record._1 + Constants.DELIMITER + record._2)
  }

  /**
    * This method reads exchange rates from provided file and represents it like a map with data as a key and rate as a value
    *
    * @param  sc                SparkContext
    * @param  exchangeRatesPath path to file from which information should be reading
    * @return map with exchange rates
    */

  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = {
    val lines = sc.textFile(exchangeRatesPath).map(line => line.split(Constants.DELIMITER))
    lines.map(line => (line(0), line(3).toDouble)).collect.toMap
  }


  /**
    * This method execute the next actions:
    * 1) filters erroneous records and for each bid leave prices only for US, CA and MX,
    * 2) transpose bids and include the related losa  in a separate column
    * and get rid of records where there is no price for Losa or price is not decimal using method <code>addPricesByLosa</code>
    * 3) converts USD to EUR and dates to proper format
    *
    * @param  rawBids       Rdd with bids data
    * @param  exchangeRates map with exchange rates
    * @return RDD with BidItem entities
    */

  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {
    val bidsWithLosas = rawBids
      .filter(!_.exists(record => record.matches(ERROR_BID_PATTERN)))
      .filter(_.size >= 16)
      .map(element => BidsWithMappedLosas(element(0), element(1),
        addPricesByLosa(Array(element(5), element(8), element(6)))))

    bidsWithLosas.flatMap(bid => bid.losasBids
      .map(b => BidItem(bid.motelId, bid.formateDate(bid.bidDate),
        b._1, bid.convertCurrency(b._2, exchangeRates(bid.bidDate)))))
  }

  /**
    * This method is used for loading motels from the provided path
    *
    * @param  sc         SparkContext
    * @param  motelsPath path to file from which motel's data should be reading
    * @return RDD with motel's ids and it's names
    */

  def getMotels(sc: SparkContext, motelsPath: String): RDD[(String, String)] = {
    sc.textFile(motelsPath)
      .map(line => line.split(Constants.DELIMITER))
      .map(motel => (motel(0), motel(1)))
  }

  /**
    * This method is used for  joining bids with motels names and and filters records with the most expensive bids
    *
    * @param  bids   RDD with BidItem entities
    * @param  motels RDD with motel's ids and it's names
    * @return RDD with EnrichedItem entities
    *
    */
  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {
    val motelsWithId = motels.map(motel => (motel._1, motel))

    val enriched = bids.map(bid => (bid.motelId, bid)).join(motelsWithId)
      .map(bid => new EnrichedItem(bid._1, bid._2._2._2, bid._2._1.bidDate, bid._2._1.loSa, bid._2._1.price))
      .map(m => (m.motelId, m.bidDate) -> m)
      .reduceByKey((k, v) => if (k.price > v.price) k else v)
      .map(bid => bid._2)

    enriched
  }

  /**
    * This method is used for adding motel's prices for locations to the map
    * It checks It checks if price is proper decimal number in other case this location will not be added using <code>checkForNumber</code>
    *
    * @param  prices array with prices
    * @return map with location as a key and price as a value
    */
  def addPricesByLosa(prices: Array[String]): Map[String, Double] = {
    var pricesByLosa = Map[String, Double]()
    var losaPosition = 0

    for (price <- prices if checkForNumber(price)) {
      pricesByLosa += (Constants.TARGET_LOSAS(losaPosition) -> price.toDouble)
      losaPosition += 1
    }

    pricesByLosa
  }

  /**
    * This method checks if price is proper decimal number
    *
    * @param  price represents price
    * @return true if price is correct and false in other case
    */
  def checkForNumber(price: String): Boolean = {
    try {
      price.toDouble
    }
    catch {
      case e: NumberFormatException => false
    }
    price != null && !price.isEmpty
  }
}
