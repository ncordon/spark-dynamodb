package com.audienceproject.spark.dynamodb.util

import com.audienceproject.shaded.google.common.util.concurrent.RateLimiter

class ResponsiveRateLimiter(private val getRate: () => Double) {
    private val rateLimiter = RateLimiter.create(getRate())

    private def refreshRateLimiter(): RateLimiter = {
        rateLimiter.setRate(getRate())
        rateLimiter
    }

    private val refreshedRateLimiter = new VariableRefresher(refreshRateLimiter)

    def acquire(permits: Int): Unit = {
        refreshedRateLimiter.get().acquire(permits)
    }
}
