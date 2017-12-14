package com.epam.hubd.spark.scala.core.homework.domain

import com.epam.hubd.spark.scala.core.homework.Constants

/**
  * This class represents entity with motels ids, bids dates and prices exploded by locations
  *
  */
case class BidsWithMappedLosas(motelId: String, bidDate: String, losasBids: Map[String, Double]) {

  /**
    * This method allows to convert USD to EUR and round this value to 3 decimal precision
    *
    * @param  price value which should be converted
    * @param  rate  rate for conversion
    * @return converted price
    */
  def convertCurrency(price: Double, rate: Double): Double = {
    "%.3f".format(price * rate).toDouble
  }

  /**
    * This method allows to convert date to proper format : from HH-dd-MM-yyyy format to yyyy-MM-dd HH:mm
    *
    * @param  date represents date which should be converted
    * @return converted date
    */

  def formateDate(date: String): String = {
    Constants.OUTPUT_DATE_FORMAT.print(Constants.INPUT_DATE_FORMAT.parseDateTime(date))
  }

  override def toString: String = s"$motelId,$bidDate,$losasBids"

}
