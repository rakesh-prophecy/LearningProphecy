package io.prophecy.pipelines.customerorders.graph

import io.prophecy.libs._
import io.prophecy.pipelines.customerorders.udfs.PipelineInitCode._
import io.prophecy.pipelines.customerorders.udfs.UDFs._
import io.prophecy.pipelines.customerorders.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object amounts_by_customer_id {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("customer_id")).agg(sum(col("amount")).as("total_amount"))

}
