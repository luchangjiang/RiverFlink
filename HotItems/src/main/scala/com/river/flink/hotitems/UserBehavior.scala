package com.river.flink.hotitems

case class UserBehavior(userId: Long, itemId: Long, categoryId: Long, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

case class UvViewCount(windowEnd: Long, count: Long)
