package com.tech.model

import java.sql.Date

/** A single FRED macro series observation — the raw payload is
 * already row-shaped (an "observations" array), so no flattening is
 * needed, just typing/cleanup.
 */
case class FredObservation(
    seriesId: String,
    date: Date,
    value: Double
)
