package com.river.flink.login

case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

case class LoginFailedView(userId: Long, firstTime: Long, lastTime: Long, msg: String)
