package com.tech.model

import java.sql.Date

/** Final Silver row: the asset's price for the day, along with the
 * last known value of each macro series (forward-fill / as-of join)
 * up to that date. Macro columns are Option because early in the
 * price history there may not be any prior observation yet.
 */
case class PriceWithMacro(
     symbol: String,
     date: Date,
     open: Double,
     high: Double,
     low: Double,
     close: Double,
     volume: Long,
     cpi: Option[Double],
     fedFundsRate: Option[Double],
     treasury10y: Option[Double]
)
