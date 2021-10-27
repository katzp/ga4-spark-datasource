package com.katzp.ga4.spark.datasource

import com.google.analytics.data.v1beta.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

case class AnalyticsPartition(reportSpec: AnalyticsReportSpec) extends InputPartition

/**
 * Factory to build readers.
 * @param schema Final Spark schema
 */
class AnalyticsPartitionReadFactory(schema: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new AnalyticsPartitionReader(partition.asInstanceOf[AnalyticsPartition], schema)
}

/**
 * Reader that operates on a partition and performs read of GA4 API and processes
 * into Spark InternalRows.
 * @param partition Partition holding GA4 report definition
 * @param schema Final Spark schema
 */
class AnalyticsPartitionReader(partition: AnalyticsPartition, schema: StructType)
    extends PartitionReader[InternalRow] {

  private var rows: List[Row] = List.empty
  private var index: Int = 0

  override def next(): Boolean = if (index == 0) true else index < rows.length

  override def get(): InternalRow = {
    if (rows.isEmpty) {
      val ga = AnalyticsReporting.authenticate(partition.reportSpec.serviceAccount)
      val request = AnalyticsReporting.buildReportRequest(partition.reportSpec)
      rows = ga.runReport(request).getRowsList().asScala.toList
      ga.close()
    }
    val gaRow = rows(index)
    index += 1
    gaRowToInternalRow(gaRow, schema)
  }

  /**
   * Take a GA4 Row and transforms into Spark InternalRow based on the schema.
   * @param row GA4 Row
   * @param schema Spark schema
   * @return Spark SQL InternalRow
   */
  private[datasource] def gaRowToInternalRow(row: Row, schema: StructType): InternalRow = {
    val dimensions = row.getDimensionValuesList.asScala.map(_.getValue).toList
    val metrics = row.getMetricValuesList.asScala.map(_.getValue).toList
    val rowValuesAsStringsWithSchema = (dimensions ++ metrics).zip(schema)
    val castedToInternalTypes = rowValuesAsStringsWithSchema.map { case (data, field) =>
      field.dataType match {
        case s: StringType    => UTF8String.fromString(data)
        case i: IntegerType   => data.toInt
        case d: DoubleType    => data.toDouble
        case l: LongType      => data.toLong
        case f: FloatType     => data.toFloat
        case t: TimestampType => DateUtils.microSecondsSinceEpoch(data).orNull
        case date: DateType   => DateUtils.daysSinceEpoch(data).orNull
      }
    }
    InternalRow.fromSeq(castedToInternalTypes)
  }

  override def close(): Unit = ()
}
