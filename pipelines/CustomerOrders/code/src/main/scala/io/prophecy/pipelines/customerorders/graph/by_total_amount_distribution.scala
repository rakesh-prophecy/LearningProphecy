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

object by_total_amount_distribution {

  def apply(
    context: Context,
    in:      DataFrame
  ): (DataFrame, DataFrame, DataFrame) =
    (in.filter(col("total_amount") < lit(5000)),
     in.filter(
       (col("total_amount") >= lit(5000)).and(col("total_amount") < lit(10000))
     ),
     in.filter(col("total_amount") >= lit(10000))
    )

}
