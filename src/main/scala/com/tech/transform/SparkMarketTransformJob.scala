package com.tech.transform

import com.tech.config.AppConfig
import com.tech.quality.DataQualityChecks
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable

/**
 * Spark job: reads Twelve Data + FRED raw data from Bronze,
 * flattens/types it, does an as-of join (forward-filling the macro
 * series over the trading calendar) and writes the Silver as Delta.
 *
 * Alpha Vantage and Stooq were dropped as sources: Alpha Vantage's
 * free tier no longer allows full daily history (outputsize=full
 * became premium), and Stooq started requiring a captcha-issued
 * apikey — neither is viable for unattended extraction anymore.
 * Twelve Data replaces both (5000 data points/request on the free
 * tier, official API).
 *
 * Join decision: the macro series (CPIAUCSL, FEDFUNDS are monthly;
 * DGS10 is daily) are propagated forward by last known value up to
 * each trading day — an exact-date join would leave most days without
 * CPI/FEDFUNDS. To switch to "only when the date matches exactly",
 * replace buildFilledMacroCalendar() with a direct join.
 *
 * The FRED series list is hardcoded in SERIES_FRED — update it if you
 * add another indicator.
 */
object SparkMarketTransformJob {

  private val TwelveDataBronzePath = "s3a://market-data-lake/bronze/twelvedata/"
  private val FredBronzePath = "s3a://market-data-lake/bronze/fred/"
  private val OutputPath = "s3a://market-data-lake/silver/price_with_macro"

  private val SERIES_FRED = Seq("CPIAUCSL", "FEDFUNDS", "DGS10")

  private val TwelveDataSchema = StructType(Seq(
    StructField("meta", StructType(Seq(
      StructField("symbol", StringType)
    ))),
    StructField("values", ArrayType(StructType(Seq(
      StructField("datetime", StringType),
      StructField("open", StringType),
      StructField("high", StringType),
      StructField("low", StringType),
      StructField("close", StringType),
      StructField("volume", StringType)
    ))))
  ))

  def main(args: Array[String]): Unit = {
    val spark = buildSparkSession()

    val prices = readTwelveDataPrices(spark)
    val filledMacro = readAndBuildMacroCalendar(spark, prices)

    val priceWithMacro = prices
      .join(filledMacro, Seq("date"), "left")
      .select(
        col("symbol"), col("date"),
        col("open"), col("high"), col("low"), col("close"), col("volume"),
        col("cpi"), col("fedFundsRate"), col("treasury10y")
      )

    runChecks(prices, priceWithMacro)

    if (DeltaTable.isDeltaTable(spark, OutputPath)) {
      val deltaTable = DeltaTable.forPath(spark, OutputPath)

      deltaTable.as("target")
        .merge(
          priceWithMacro.as("source"),
          "target.symbol = source.symbol AND target.date = source.date"
        )
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()

      println(s"MERGE completed on Silver → $OutputPath")
    } else {
      priceWithMacro.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("symbol")
        .save(OutputPath)

      println(s"First write of Silver → $OutputPath")
    }

    println(s"Write completed at: $OutputPath")

    spark.stop()
  }

  /** Reads Twelve Data's Bronze payload (one JSON file per symbol,
   * a "values" array with one entry per trading day) and flattens
   * it into one row per (symbol, date).
   */
  private def readTwelveDataPrices(spark: SparkSession): DataFrame = {
    spark.read
      .schema(TwelveDataSchema)
      .json(TwelveDataBronzePath)
      .select(
        col("meta.symbol").as("symbol"),
        explode(col("values")).as("v")
      )
      .select(
        col("symbol"),
        to_date(col("v.datetime")).as("date"),
        col("v.open").cast(DoubleType).as("open"),
        col("v.high").cast(DoubleType).as("high"),
        col("v.low").cast(DoubleType).as("low"),
        col("v.close").cast(DoubleType).as("close"),
        col("v.volume").cast(LongType).as("volume")
      )
  }

  /** Reads FRED's Bronze payload (an "observations" array, already
   * row-shaped), pivots it into one column per series, and forward-
   * fills the last known value onto every day of the trading
   * calendar.
   */
  private def readAndBuildMacroCalendar(
                                         spark: SparkSession,
                                         prices: DataFrame
                                       ): DataFrame = {
    import spark.implicits._

    val fredLong = spark.read
      .option("multiLine", value = true)
      .json(FredBronzePath)
      .select(
        regexp_extract(input_file_name(), "([^/]+)\\.json$", 1).as("seriesId"),
        explode(col("observations")).as("obs")
      )
      .select(
        col("seriesId"),
        to_date(col("obs.date")).as("date"),
        col("obs.value").as("valueStr")
      )
      .filter(col("valueStr") =!= ".")
      .withColumn("value", col("valueStr").cast(DoubleType))

    val fredWide = fredLong
      .groupBy("date")
      .pivot("seriesId", SERIES_FRED)
      .agg(first("value"))
      .select(
        col("date"),
        col("CPIAUCSL").as("cpi"),
        col("FEDFUNDS").as("fedFundsRate"),
        col("DGS10").as("treasury10y")
      )

    val minDate = fredWide.agg(min("date")).as[java.sql.Date].head()
    val maxDate = prices.agg(max("date")).as[java.sql.Date].head()

    val calendar = spark.sql(
      s"select explode(sequence(to_date('$minDate'), to_date('$maxDate'), interval 1 day)) as date"
    )

    val window = Window.orderBy("date")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    calendar
      .join(fredWide, Seq("date"), "left")
      .withColumn("cpi", last(col("cpi"), ignoreNulls = true).over(window))
      .withColumn("fedFundsRate", last(col("fedFundsRate"), ignoreNulls = true).over(window))
      .withColumn("treasury10y", last(col("treasury10y"), ignoreNulls = true).over(window))
  }

  private def runChecks(prices: DataFrame, priceWithMacro: DataFrame): Unit = {
    val results = Seq(
      DataQualityChecks.noNullsIn(prices, Seq("symbol", "date", "open", "high", "low", "close")),
      DataQualityChecks.noDuplicatesIn(prices, Seq("symbol", "date")),
      DataQualityChecks.positivePrices(prices),
      DataQualityChecks.highGreaterOrEqualLow(prices),
      DataQualityChecks.nonNegativeVolume(prices)
    )
    DataQualityChecks.printReport(results)

    if (results.exists(!_.passed)) {
      println("WARNING: one or more quality checks failed — see report above.")
    }
  }

  private def buildSparkSession(): SparkSession = {

    SparkSession.builder()
      .appName("market-data-lake-transform")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.hadoop.fs.s3a.access.key", AppConfig.accessKey)
      .config("spark.hadoop.fs.s3a.secret.key", AppConfig.secretKey)
      .config("spark.hadoop.fs.s3a.endpoint",   AppConfig.endpoint)
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