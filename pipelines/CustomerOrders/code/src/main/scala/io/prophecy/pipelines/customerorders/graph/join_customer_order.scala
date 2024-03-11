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

object join_customer_order {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.customer_id") === col("in1.customer_id"),
            "inner"
      )
      .select(
        col("in0.customer_id").as("customer_id"),
        col("in0.first_name").as("first_name"),
        col("in0.last_name").as("last_name"),
        col("in0.country_code").as("country_code"),
        col("in1.order_id").as("order_id"),
        col("in1.order_status").as("order_status"),
        col("in1.order_category").as("order_category"),
        col("in1.order_date").as("order_date"),
        col("in1.amount").as("amount")
      )

}
