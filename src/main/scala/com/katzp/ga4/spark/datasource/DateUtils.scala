package com.katzp.ga4.spark.datasource

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import scala.util.Try

/**
 * Helper functions to translate a date field from GA4 API to Spark InternalRow formats.
 */
object DateUtils {

  final val gaDateFormat = "yyyyMMdd"
  final val timestampFormat = "yyyyMMddHHmmss"

  /**
   * Takes a string date "yyyyMMdd" and returns integer of days since epoch.
   */
  def daysSinceEpoch(date: String): Option[Int] = {
    val formatter = DateTimeFormatter.ofPattern(gaDateFormat)
    Try {
      val d = LocalDate.parse(date, formatter)
      d.toEpochDay.toInt
    }.toOption
  }

  /**
   * Takes string datetime of "yyyyMMddHHmmss", "yyyyMMddHH" or "yyyyMMddHHmm"
   * and returns microseconds since epoch.
   */
  def microSecondsSinceEpoch(dateTime: String): Option[Long] = {
    val date = dateTime.length match {
      case 10 => dateTime + "0000"
      case 12 => dateTime + "00"
      case _  => dateTime
    }
    val formatter2 = DateTimeFormatter.ofPattern(timestampFormat)
    Try {
      val d: LocalDateTime = LocalDateTime.parse(date, formatter2)
      d.toEpochSecond(ZoneOffset.UTC) * 1000000L
    }.toOption
  }
}
