package com.audienceproject.spark.dynamodb.util

import java.time.Clock.tick
import java.time.{Clock, Duration, Instant}
import scala.concurrent.duration.{DurationInt, FiniteDuration}

class VariableRefresher[T](updatedValue: () => T, frequency: FiniteDuration = 5.minutes ) {
    private val clock = Clock.systemUTC()
    private val refreshFreq = Duration.ofMillis(frequency.toMillis)
    private var lastUpdated = Instant.now(clock)
    private var current = updatedValue()

    def get(): T = {
        val nowTick = tick(clock, refreshFreq).instant()

        if (nowTick.isAfter(lastUpdated)) {
            current = updatedValue()
            lastUpdated = nowTick
        }

        current
    }
}
