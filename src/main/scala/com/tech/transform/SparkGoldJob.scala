package com.tech.transform


import com.tech.config.AppConfig
import com.tech.quality.DataQualityChecks
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Gold layer com MERGE (upsert).
 *
 * - daily_features: MERGE por (symbol, date)
 * - symbol_summary: MERGE por symbol (sempre a versão mais recente)
 */
object SparkGoldJob {

  private val SilverPath       = "s3a://market-data-lake/silver/price_with_macro"
  private val GoldDailyPath    = "s3a://market-data-lake/gold/daily_features"
  private val GoldSummaryPath  = "s3a://market-data-lake/gold/symbol_summary"

  def main(args: Array[String]): Unit = {
    val spark = buildSparkSession()

    val silver = spark.read.format("delta").load(SilverPath)
    val dailyFeatures = buildDailyFeatures(silver)

    runGoldChecks(dailyFeatures)

    // ---- daily_features (MERGE) ----
    upsertDailyFeatures(spark, dailyFeatures)

    // ---- symbol_summary (MERGE) ----
    val summary = buildSymbolSummary(dailyFeatures)
    upsertSymbolSummary(spark, summary)

    spark.stop()
  }

  // ------------------------------------------------------------------
  // Features
  // ------------------------------------------------------------------

  private def buildDailyFeatures(silver: DataFrame): DataFrame = {
    val w   = Window.partitionBy("symbol").orderBy("date")
    val w20 = w.rowsBetween(-19, 0)
    val w50 = w.rowsBetween(-49, 0)
    val w200 = w.rowsBetween(-199, 0)
    val w21 = w.rowsBetween(-20, 0)
    val w63 = w.rowsBetween(-62, 0)

    val delta = col("close") - lag("close", 1).over(w)
    val gain  = when(delta > 0, delta).otherwise(0.0)
    val loss  = when(delta < 0, -delta).otherwise(0.0)

    val avgGain = avg(gain).over(w.rowsBetween(-13, 0))
    val avgLoss = avg(loss).over(w.rowsBetween(-13, 0))
    val rs      = avgGain / avgLoss
    val rsi = when(avgLoss === 0, lit(100.0))
      .otherwise(lit(100) - (lit(100) / (lit(1) + rs)))

    silver
      .withColumn("return_1d",  (col("close") / lag("close", 1).over(w)) - 1)
      .withColumn("return_5d",  (col("close") / lag("close", 5).over(w)) - 1)
      .withColumn("return_21d", (col("close") / lag("close", 21).over(w)) - 1)
      .withColumn("return_63d", (col("close") / lag("close", 63).over(w)) - 1)

      .withColumn("sma_20",  avg("close").over(w20))
      .withColumn("sma_50",  avg("close").over(w50))
      .withColumn("sma_200", avg("close").over(w200))

      .withColumn("dist_sma_20",  (col("close") / col("sma_20")) - 1)
      .withColumn("dist_sma_50",  (col("close") / col("sma_50")) - 1)
      .withColumn("dist_sma_200", (col("close") / col("sma_200")) - 1)

      .withColumn("vol_21d",     stddev("return_1d").over(w21))
      .withColumn("vol_63d",     stddev("return_1d").over(w63))
      .withColumn("vol_ann_21d", col("vol_21d") * sqrt(lit(252)))

      .withColumn("avg_volume_20d", avg("volume").over(w20))
      .withColumn("volume_rel_20d", col("volume") / col("avg_volume_20d"))

      .withColumn("rsi_14", rsi)

      .withColumn("high_252d",    max("high").over(w.rowsBetween(-251, 0)))
      .withColumn("drawdown_252d", (col("close") / col("high_252d")) - 1)

      .withColumn("cpi_chg_1m",      (col("cpi") / lag("cpi", 21).over(w)) - 1)
      .withColumn("fed_funds_chg",   col("fedFundsRate") - lag("fedFundsRate", 1).over(w))
      .withColumn("treasury10y_chg", col("treasury10y") - lag("treasury10y", 1).over(w))

      .select(
        col("symbol"), col("date"),
        col("open"), col("high"), col("low"), col("close"), col("volume"),
        col("cpi"), col("fedFundsRate"), col("treasury10y"),
        col("return_1d"), col("return_5d"), col("return_21d"), col("return_63d"),
        col("sma_20"), col("sma_50"), col("sma_200"),
        col("dist_sma_20"), col("dist_sma_50"), col("dist_sma_200"),
        col("vol_21d"), col("vol_63d"), col("vol_ann_21d"),
        col("volume_rel_20d"),
        col("rsi_14"),
        col("drawdown_252d"),
        col("cpi_chg_1m"), col("fed_funds_chg"), col("treasury10y_chg")
      )
  }

  private def buildSymbolSummary(daily: DataFrame): DataFrame = {
    val w = Window.partitionBy("symbol").orderBy(col("date").desc)

    daily
      .withColumn("rn", row_number().over(w))
      .filter(col("rn") === 1)
      .select(
        col("symbol"),
        col("date").as("as_of_date"),
        col("close").as("last_close"),
        col("return_21d").as("return_1m"),
        col("return_63d").as("return_3m"),
        col("vol_ann_21d").as("vol_ann_1m"),
        col("rsi_14"),
        col("drawdown_252d"),
        col("dist_sma_50"),
        col("dist_sma_200"),
        col("cpi"),
        col("fedFundsRate"),
        col("treasury10y")
      )
  }

  // ------------------------------------------------------------------
  // MERGE helpers
  // ------------------------------------------------------------------

  private def upsertDailyFeatures(spark: SparkSession, updates: DataFrame): Unit = {
    if (DeltaTable.isDeltaTable(spark, GoldDailyPath)) {
      val deltaTable = DeltaTable.forPath(spark, GoldDailyPath)

      deltaTable.as("target")
        .merge(
          updates.as("source"),
          "target.symbol = source.symbol AND target.date = source.date"
        )
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()

      println(s"MERGE completed on daily_features → $GoldDailyPath")
    } else {
      // primeira execução
      updates.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("symbol")
        .option("overwriteSchema", "true")
        .save(GoldDailyPath)

      println(s"First write of daily_features → $GoldDailyPath")
    }
  }

  private def upsertSymbolSummary(spark: SparkSession, updates: DataFrame): Unit = {
    if (DeltaTable.isDeltaTable(spark, GoldSummaryPath)) {
      val deltaTable = DeltaTable.forPath(spark, GoldSummaryPath)

      deltaTable.as("target")
        .merge(
          updates.as("source"),
          "target.symbol = source.symbol"
        )
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()

      println(s"MERGE completed on symbol_summary → $GoldSummaryPath")
    } else {
      updates.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GoldSummaryPath)

      println(s"First write of symbol_summary → $GoldSummaryPath")
    }
  }

  // ------------------------------------------------------------------
  // Quality + Session
  // ------------------------------------------------------------------

  private def runGoldChecks(df: DataFrame): Unit = {
    val results = Seq(
      DataQualityChecks.noNullsIn(df, Seq("symbol", "date", "close")),
      DataQualityChecks.noDuplicatesIn(df, Seq("symbol", "date")),
      DataQualityChecks.positivePrices(df)
    )
    DataQualityChecks.printReport(results)

    if (results.exists(!_.passed)) {
      println("WARNING: one or more Gold quality checks failed.")
    }
  }

  private def buildSparkSession(): SparkSession = {

    SparkSession.builder()
      .appName("market-data-lake-gold")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.hadoop.fs.s3a.access.key", AppConfig.accessKey)
      .config("spark.hadoop.fs.s3a.secret.key", AppConfig.secretKey)
      .config("spark.hadoop.fs.s3a.endpoint", AppConfig.endpoint)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
      .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
      .getOrCreate()
  }
}