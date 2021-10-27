package com.katzp.ga4.spark.datasource

import com.google.analytics.data.v1beta._
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.auth.oauth2.GoogleCredentials

import java.io.ByteArrayInputStream
import scala.collection.JavaConverters.asJavaIterableConverter

case class AnalyticsReportSpec(
    dimensions: Array[String],
    metrics: Array[String],
    propertyId: String,
    requestOffset: Long = 0L,
    serviceAccount: String,
    startDate: Option[String],
    endDate: Option[String]
)

/**
 * Static methods for authenticating and building report requests.
 */
object AnalyticsReporting {

  final val RESULT_LIMIT: Int = 10000

  /**
   * Authenticates and returns GA4 client.
   * @param stringCredentials JSON service account info
   * @return GA4 client
   */
  def authenticate(stringCredentials: String): BetaAnalyticsDataClient = {
    try {
      val credentials =
        GoogleCredentials.fromStream(new ByteArrayInputStream(stringCredentials.getBytes))
      val settings = BetaAnalyticsDataSettings
        .newBuilder()
        .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
        .build()
      BetaAnalyticsDataClient.create(settings)
    } catch {
      case e: Throwable => throw AnalyticsAuthException(s"Error authenticating: $e")
    }
  }

  private[datasource] def addDimensions(dims: Array[String]): List[Dimension] =
    dims.map(d => Dimension.newBuilder().setName(d).build()).toList

  private[datasource] def addMetrics(metrics: Array[String]): List[Metric] =
    metrics.map(m => Metric.newBuilder().setName(m).build()).toList

  /**
   * Builds out report request based on passed in definition.
   * @param reportSpec GA4 report definition
   * @return GA4 report request
   */
  def buildReportRequest(reportSpec: AnalyticsReportSpec): RunReportRequest = {
    val request = RunReportRequest
      .newBuilder()
      .addAllMetrics(addMetrics(reportSpec.metrics).asJava)
      .addAllDimensions(addDimensions(reportSpec.dimensions).asJava)
      .setProperty(s"properties/${reportSpec.propertyId}")
      .setLimit(RESULT_LIMIT)
      .setOffset(reportSpec.requestOffset)

    if (reportSpec.startDate.isDefined && reportSpec.endDate.isDefined)
      request.addDateRanges(
        DateRange
          .newBuilder()
          .setStartDate(reportSpec.startDate.get)
          .setEndDate(reportSpec.endDate.get)
          .build()
      )

    request.build()
  }
}
