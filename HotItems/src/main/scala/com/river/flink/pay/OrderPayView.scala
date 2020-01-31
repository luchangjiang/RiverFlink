package com.river.flink.pay

case class OrderPayEvent (orderId: String, payStatus: String, payId: String, eventTime: Long)

case class ReceiptEvent(payId: String, payType: String, eventTime: Long)

case class PayResult(orderId: String, msg: String)
