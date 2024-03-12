package io.prophecy.pipelines.customerorders.graph

import io.prophecy.libs._
import io.prophecy.pipelines.customerorders.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object SuperPremiumCustomers {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .mode("append")
      .save(
        "dbfs:/Prophecy/3e1dbccaa39cc079b8333d20f9b4b0fa/SuperPremiumCustomers.csv"
      )

}
