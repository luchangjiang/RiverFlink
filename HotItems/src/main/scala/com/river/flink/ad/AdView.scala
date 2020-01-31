package com.river.flink.ad

case class AdClickEvent (userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class ProvinceViewCount (windowEnd: Long, province: String, count: Long)

case class BlackListWarning(userId: Long, adId: Long, msg: String)


