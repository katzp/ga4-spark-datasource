package com.katzp.ga4.spark.datasource

import com.google.analytics.data.v1beta.{FilterExpressionList, Filter => GAFilter}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Builds scan and updates GA4 report definition based on column pruning & pushdown filters.
 * @param schema Initial logical schema
 * @param options User input Spark options
 * @param reportingSpec GA4 report definition
 */
class AnalyticsScanBuilder(
    schema: StructType,
    options: CaseInsensitiveStringMap,
    reportingSpec: AnalyticsReportSpec
) extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters {

  private var finalSchema = schema
  private var pushDownFilters = Array[Filter]()

  /**
   * Applies column pruning and adds filters to the actual GA reporting spec that we
   * will send to the PartitionReaders to perform the API requests.
   *
   * @param spec Container to hold info on the GA4 report
   * @return Container to hold info on the GA4 report with column pruning & pushdown filters applied
   */
  private def filterReportingSpec(spec: AnalyticsReportSpec): AnalyticsReportSpec = {
    val finalColumns = finalSchema.fields.map(_.name)
    spec.copy(
      dimensions = spec.dimensions.filter(c => finalColumns.contains(c)),
      metrics = spec.metrics.filter(c => finalColumns.contains(c))
    )
  }

  override def build(): Scan =
    new AnalyticsScan(finalSchema, options, filterReportingSpec(reportingSpec))

  override def pruneColumns(requiredSchema: StructType): Unit =
    finalSchema = if (requiredSchema.length > 0) requiredSchema else schema

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    filters
  }

  override def pushedFilters(): Array[Filter] = pushDownFilters

  private[datasource] def buildGA4FilterExpression(filters: Array[Filter]): FilterExpressionList =
    ???
}

/**
 * Representation of a Spark Scan
 * @param schema Updated inferred schema
 * @param options User input Spark options
 * @param reportingSpec Updated GA4 report definition
 */
class AnalyticsScan(
    schema: StructType,
    options: CaseInsensitiveStringMap,
    reportingSpec: AnalyticsReportSpec
) extends Scan
    with Batch {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = getPartitionsBasedOnOffsets()

  override def createReaderFactory(): PartitionReaderFactory = new AnalyticsPartitionReadFactory(
    schema
  )

  /**
   * Performs reporting request on driver node simply to get the row count and determine how many
   * pages the report contains. The execution nodes will create 1 partition per page and later execute
   * the reporting request for the page.
   * @return Array of analytics partitions containing all the info needed to perform requests
   *         and read pages on the executors
   */
  private[datasource] def getPartitionsBasedOnOffsets(): Array[InputPartition] = {
    val request = AnalyticsReporting.buildReportRequest(reportingSpec)
    val ga = AnalyticsReporting.authenticate(reportingSpec.serviceAccount)
    val rows = ga.runReport(request).getRowCount.toDouble
    ga.close()
    val numPartitions = Math.ceil(rows / AnalyticsReporting.RESULT_LIMIT.toDouble).toInt
    (0 until numPartitions).map { idx =>
      AnalyticsPartition(reportingSpec.copy(requestOffset = idx * AnalyticsReporting.RESULT_LIMIT))
    }.toArray
  }
}
