package com.katzp.ga4.spark.datasource

import org.scalatest.flatspec.AnyFlatSpec

class DateUtilsSpec extends AnyFlatSpec {
  "days since epoch utility" should "return correct days" in {
    val oneDay = DateUtils.daysSinceEpoch("19700102")
    assert(oneDay.get == 1)
    val thirtyOneDays = DateUtils.daysSinceEpoch("19700201")
    assert(thirtyOneDays.get == 31)
  }

  "microseconds since epoch utility" should "return correct value" in {
    val withHour = DateUtils.microSecondsSinceEpoch("1970010101")
    val withMinute = DateUtils.microSecondsSinceEpoch("197001010001")
    assert(withHour.get == (60L * 60L * 1000000L))
    assert(withMinute.get == 60000000L)
  }
}
