package com.river.flink.marketing

case class MarketingUserBehavior (userId: String, behavior: String, channel: String, timestamp: Long)

case class MarketingViewChannelCount (windowStart: String, windowEnd: String, behavior: String, channel: String, count: Long)

case class MarketingViewCount (windowStart: String, windowEnd: String, count: Long)