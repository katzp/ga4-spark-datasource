package com.katzp.ga4.spark.datasource

case class AnalyticsAuthException(message: String) extends Exception(message)
case class AnalyticsMissingPropertyException(message: String) extends Exception(message)