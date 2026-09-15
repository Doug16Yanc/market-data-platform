package com.tech.transform

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Spark job: reads Alpha Vantage + FRED raw data from Bronze,
 * flattens/types it, does an as-of join (forward-filling the macro
 * series over the trading calendar) and writes the Silver as Delta.
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

  private val AlphaVantageBronzePath = "s3a://market-data-lake/bronze/alpha_vantage/"
  private val FredBronzePath = "s3a://market-data-lake/bronze/fred/"
  private val OutputPath = "s3a://market-data-lake/silver/price_with_macro"

  private val SERIES_FRED = Seq("CPIAUCSL", "FEDFUNDS", "DGS10")

  private val AlphaVantageSchema = StructType(Seq(
    StructField("Meta Data", StructType(Seq(
      StructField("2. Symbol", StringType)
    ))),
    StructField("Time Series (Daily)", MapType(StringType, StructType(Seq(
      StructField("1. open", StringType),
      StructField("2. high", StringType),
      StructField("3. low", StringType),
      StructField("4. close", StringType),
      StructField("5. volume", StringType)
    ))))
  ))

  def main(args: Array[String]): Unit = {
    val spark = buildSparkSession()

    val prices = readAlphaVantagePrices(spark)
    val filledMacro = readAndBuildMacroCalendar(spark, prices)

    val priceWithMacro = prices
      .join(filledMacro, Seq("date"), "left")
      .select(
        col("symbol"), col("date"),
        col("open"), col("high"), col("low"), col("close"), col("volume"),
        col("cpi"), col("fedFundsRate"), col("treasury10y")
      )

    runChecks(prices, priceWithMacro)

    priceWithMacro.write
      .format("delta")
      .mode("overwrite") // TODO: same note as the other jobs — switch to MERGE when it makes sense
      .partitionBy("symbol")
      .save(OutputPath)

    println(s"Write completed at: $OutputPath")

    spark.stop()
  }

  /** Reads Alpha Vantage's Bronze payload (dates as map keys, as
   * returned natively) and flattens it into one row per trading day.
   */
  private def readAlphaVantagePrices(spark: SparkSession): DataFrame = {
    spark.read
      .schema(AlphaVantageSchema)
      .json(AlphaVantageBronzePath)
      .select(
        col("`Meta Data`.`2. Symbol`").as("symbol"),
        explode(col("`Time Series (Daily)`")).as(Seq("dateStr", "values"))
      )
      .select(
        col("symbol"),
        to_date(col("dateStr")).as("date"),
        col("values.`1. open`").cast(DoubleType).as("open"),
        col("values.`2. high`").cast(DoubleType).as("high"),
        col("values.`3. low`").cast(DoubleType).as("low"),
        col("values.`4. close`").cast(DoubleType).as("close"),
        col("values.`5. volume`").cast(LongType).as("volume")
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
        // derives the seriesId from the file name
        // (bronze/fred/{seriesId}.json), since the API payload
        // doesn't repeat the id on every observation
        regexp_extract(input_file_name(), "([^/]+)\\.json$", 1).as("seriesId"),
        explode(col("observations")).as("obs")
      )
      .select(
        col("seriesId"),
        to_date(col("obs.date")).as("date"),
        col("obs.value").as("valueStr")
      )
      // FRED uses "." for a missing observation instead of omitting the row
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

    // Continuous calendar (every day, not just trading days or days
    // with a macro observation) spanning from the start of the macro
    // series to the last price date — needed for forward-fill to work
    // even when a trading date doesn't line up with an observation.
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

    // Just logs for now — decide later whether any of these checks
    // should abort the job (e.g. positivePrices failing is a strong
    // signal of broken parsing, might be worth a throw there).
    if (results.exists(!_.passed)) {
      println("WARNING: one or more quality checks failed — see report above.")
    }
  }

  private def buildSparkSession(): SparkSession = {
    val accessKey = sys.env.getOrElse("FLOCI_ACCESS_KEY", "test")
    val secretKey = sys.env.getOrElse("FLOCI_SECRET_KEY", "test")
    val endpoint = sys.env.getOrElse("FLOCI_ENDPOINT", "http://localhost:4566")

    SparkSession.builder()
      .appName("market-data-lake-transform")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.hadoop.fs.s3a.access.key", accessKey)
      .config("spark.hadoop.fs.s3a.secret.key", secretKey)
      .config("spark.hadoop.fs.s3a.endpoint", endpoint)
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