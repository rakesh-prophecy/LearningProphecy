package io.prophecy.pipelines.customerorders

import io.prophecy.libs._
import io.prophecy.pipelines.customerorders.config._
import io.prophecy.pipelines.customerorders.udfs.UDFs._
import io.prophecy.pipelines.customerorders.udfs.PipelineInitCode._
import io.prophecy.pipelines.customerorders.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_Customers = Customers(context)
    val df_Orders    = Orders(context)
    val df_join_customer_order =
      join_customer_order(context, df_Customers, df_Orders)
    val df_amounts_by_customer_id =
      amounts_by_customer_id(context, df_join_customer_order)
    val df_customers_total_amount =
      customers_total_amount(context, df_amounts_by_customer_id)
    val (df_by_total_amount_distribution_out0,
         df_by_total_amount_distribution_out1,
         df_by_total_amount_distribution_out2
    ) = by_total_amount_distribution(context, df_customers_total_amount)
    SuperPremiumCustomers(context, df_by_total_amount_distribution_out2)
    BasicCustomers(context,        df_by_total_amount_distribution_out0)
    PremiumCustomers(context,      df_by_total_amount_distribution_out1)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    val context = Context(spark, config)
    spark.conf.set("spark.sql.shuffle.partitions",   "5")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/CustomerOrders")
    registerUDFs(spark)
    try MetricsCollector.start(spark,
                               "pipelines/CustomerOrders",
                               context.config
    )
    catch {
      case _: Throwable =>
        MetricsCollector.start(spark, "pipelines/CustomerOrders")
    }
    apply(context)
    MetricsCollector.end(spark)
  }

}
