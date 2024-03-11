package io.prophecy.pipelines.customerorders.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
