package com.katzp.ga4.spark.datasource

import com.google.analytics.data.v1beta.{DimensionMetadata, MetricMetadata, MetricType}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsScalaMapConverter, setAsJavaSetConverter}

/**
 * Entrance to using GA4 as a Spark datasource.
 * Provides method to create logical table & perform schema inferance.
 */
class GA4DataSource extends TableProvider {

  /**
   * Downloads GA4 property metadata and maps GA4 schema to Spark schema
   * for dimensions and metrics passed in as Spark options.
   * @param options User passed in Spark options
   * @return Schema
   */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if (!hasRequiredProperties(options)) {
      throw AnalyticsMissingPropertyException(
        "Define serviceAccount, propertyId, dimensions & metrics as spark options"
      )
    } else {
      val reportingSpec = sparkOptionsToReportSpec(options.asCaseSensitiveMap())
      val ga = AnalyticsReporting.authenticate(reportingSpec.serviceAccount)
      val propertyMetaData = ga.getMetadata(s"properties/${reportingSpec.propertyId}/metadata")
      ga.close()
      val dimMetaData = propertyMetaData.getDimensionsList.asScala
      val metricMetaData = propertyMetaData.getMetricsList.asScala
      val dimSchema = reportingSpec.dimensions.foldLeft(StructType(Nil)) { (struct, dim) =>
        val metadata: Option[DimensionMetadata] = dimMetaData.find(_.getApiName == dim)
        if (metadata.isEmpty)
          throw AnalyticsMissingPropertyException(s"Dimension $dim does not exist.")
        else struct.add(gaDimensionToSparkType(metadata.get))
      }
      val finalSchema = reportingSpec.metrics.foldLeft(dimSchema) { (struct, m) =>
        val metadata: Option[MetricMetadata] = metricMetaData.find(_.getApiName == m)
        if (metadata.isEmpty) throw AnalyticsMissingPropertyException(s"Metric $m does not exist.")
        else struct.add(gaMetricToSparkType(metadata.get))
      }
      finalSchema
    }
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = new AnalyticsTable(schema, sparkOptionsToReportSpec(properties))

  /**
   * Maps user input Spark options to a case class holding GA4 report definition
   * that gets passed along the datasource process all the way through to PartitionReaders.
   * @param properties User passed in Spark options
   * @return GA4 report definition
   */
  private def sparkOptionsToReportSpec(properties: util.Map[String, String]): AnalyticsReportSpec =
    AnalyticsReportSpec(
      dimensions = properties.get("dimensions").split(",").map(_.trim),
      metrics = properties.get("metrics").split(",").map(_.trim),
      propertyId = properties.get("propertyId"),
      serviceAccount = properties.get("serviceAccount"),
      startDate = properties.asScala.get("startDate"),
      endDate = properties.asScala.get("endDate")
    )

  /**
   * Maps GA4 dimension to Spark field for building out inferred schema.
   * All field marked as String aside from date columns.
   */
  private def gaDimensionToSparkType(dimension: DimensionMetadata): StructField =
    dimension.getApiName match {
      case "date"           => StructField(dimension.getApiName, DateType)
      case "dateHour"       => StructField(dimension.getApiName, TimestampType)
      case "dateHourMinute" => StructField(dimension.getApiName, TimestampType)
      case _                => StructField(dimension.getApiName, StringType)
    }

  /**
   * Maps GA4 metric type to Spark field for building out inferred schema.
   * @param metric GA4 metric metadata
   * @return Spark StructField
   */
  private def gaMetricToSparkType(metric: MetricMetadata): StructField = metric.getType match {
    case MetricType.METRIC_TYPE_UNSPECIFIED => StructField(metric.getApiName, StringType)
    case MetricType.TYPE_INTEGER            => StructField(metric.getApiName, IntegerType)
    case MetricType.TYPE_FLOAT              => StructField(metric.getApiName, FloatType)
    case MetricType.TYPE_SECONDS            => StructField(metric.getApiName, LongType)
    case MetricType.TYPE_MILLISECONDS       => StructField(metric.getApiName, LongType)
    case MetricType.TYPE_MINUTES            => StructField(metric.getApiName, LongType)
    case MetricType.TYPE_HOURS              => StructField(metric.getApiName, LongType)
    case MetricType.TYPE_STANDARD           => StructField(metric.getApiName, LongType)
    case MetricType.TYPE_CURRENCY           => StructField(metric.getApiName, StringType)
    case MetricType.TYPE_FEET               => StructField(metric.getApiName, LongType)
    case MetricType.TYPE_MILES              => StructField(metric.getApiName, LongType)
    case MetricType.TYPE_METERS             => StructField(metric.getApiName, LongType)
    case MetricType.TYPE_KILOMETERS         => StructField(metric.getApiName, LongType)
    case MetricType.UNRECOGNIZED            => StructField(metric.getApiName, StringType)
  }

  /**
   * Validation to ensure user passed in needed information to Spark.
   */
  private[datasource] def hasRequiredProperties(options: CaseInsensitiveStringMap): Boolean = {
    val requiredOptions = Seq("dimensions", "metrics", "serviceAccount", "propertyId")
    if (requiredOptions.forall(options.containsKey(_))) true
    else false
  }
}

/**
 * Logical representation of GA4 table.
 * @param schema Will always be inferred schema from GA4DataSource
 * @param spec GA4 report definition
 */
class AnalyticsTable(schema: StructType, spec: AnalyticsReportSpec)
    extends Table
    with SupportsRead {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new AnalyticsScanBuilder(schema, options, spec)

  override def name(): String = "google-analytics"

  override def schema(): StructType = schema

  /**
   * This is purely a batch read data source. No writes or streaming read/write capabilities.
   */
  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava
}
