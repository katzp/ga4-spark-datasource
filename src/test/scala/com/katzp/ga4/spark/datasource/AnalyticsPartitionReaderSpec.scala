package com.katzp.ga4.spark.datasource

import com.google.analytics.data.v1beta.{DimensionValue, MetricValue, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class AnalyticsPartitionReaderSpec extends AnyFlatSpec with BeforeAndAfter {

  val gaRow = Row
    .newBuilder()
    .addDimensionValues(DimensionValue.newBuilder().setValue("USA"))
    .addDimensionValues(DimensionValue.newBuilder().setValue("19700102"))
    .addMetricValues(MetricValue.newBuilder().setValue("10"))
    .build()

  val schema =
    StructType(
      Seq(
        StructField("country", StringType),
        StructField("date", DateType),
        StructField("users", IntegerType)
      )
    )

  val partition = AnalyticsPartition(
    AnalyticsReportSpec(Array(""), Array(""), "", 0L, "", None, None)
  )
  val reader = new AnalyticsPartitionReadFactory(schema)
    .createReader(partition)
    .asInstanceOf[AnalyticsPartitionReader]

  "PartitionReader" should "return correct number of dates since epoch" in {
    val internalRow = reader.gaRowToInternalRow(gaRow, schema)
    assert(internalRow == InternalRow(UTF8String.fromString("USA"), 1, 10))
  }
}
