package com.tech.quality

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Simple quality checks, no external framework (Deequ/Great
 * Expectations) — each check just counts violations and prints a
 * report; none of them fail the job on their own. Whoever calls
 * these decides what to do with the report (abort, alert, just log)
 * — see SparkMarketTransformJob.runChecks().
 */
object DataQualityChecks {

  case class CheckResult(name: String, violatingRows: Long) {
    def passed: Boolean = violatingRows == 0
  }

  /** No row should be null in the required columns. */
  def noNullsIn(df: DataFrame, columns: Seq[String]): CheckResult = {
    val condition = columns.map(c => col(c).isNull).reduce(_ || _)
    val violating = df.filter(condition).count()
    CheckResult(s"no_nulls_in(${columns.mkString(",")})", violating)
  }

  /** No duplicates on the business key (e.g. symbol+date). */
  def noDuplicatesIn(df: DataFrame, key: Seq[String]): CheckResult = {
    val duplicated = df.groupBy(key.map(col): _*)
      .count()
      .filter(col("count") > 1)
      .count()
    CheckResult(s"no_duplicates_in(${key.mkString(",")})", duplicated)
  }

  /** Prices (open/high/low/close) must be positive. */
  def positivePrices(df: DataFrame): CheckResult = {
    val violating = df.filter(
      col("open") <= 0 || col("high") <= 0 || col("low") <= 0 || col("close") <= 0
    ).count()
    CheckResult("positive_prices", violating)
  }

  /** high must be >= low (and >= open/close) — a malformed payload or
   * a swapped field during extraction usually shows up here first.
   */
  def highGreaterOrEqualLow(df: DataFrame): CheckResult = {
    val violating = df.filter(col("high") < col("low")).count()
    CheckResult("high_greater_or_equal_low", violating)
  }

  /** volume can't be negative (zero is fine — a no-trading day
   * sometimes shows up that way instead of being absent).
   */
  def nonNegativeVolume(df: DataFrame): CheckResult = {
    val violating = df.filter(col("volume") < 0).count()
    CheckResult("non_negative_volume", violating)
  }

  def printReport(results: Seq[CheckResult]): Unit = {
    println("===== QUALITY REPORT =====")
    results.foreach { r =>
      val status = if (r.passed) "OK" else s"FAILED (${r.violatingRows} rows)"
      println(s"  ${r.name}: $status")
    }
  }
}