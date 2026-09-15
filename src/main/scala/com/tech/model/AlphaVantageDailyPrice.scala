package com.tech.model

import java.sql.Date

/** Daily price for an asset, already flattened out of the
 * "Time Series (Daily)" payload from Alpha Vantage.
 */
case class AlphaVantageDailyPrice(
   symbol: String,
   date: Date,
   open: Double,
   high: Double,
   low: Double,
   close: Double,
   volume: Long
)
