package com.river.flink.pay

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoinHandler {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val payPath = getClass.getResource("/OrderLog.csv")
    val payStream = env.readTextFile(payPath.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderPayEvent(dataArray(0), dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .filter(_.payStatus=="pay")
      .assignAscendingTimestamps(_.eventTime*1000L)
      .keyBy(_.payId)

    val receiptPath = getClass.getResource("/ReceiptLog.csv")
    val receiptStream = env.readTextFile(receiptPath.getPath)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.eventTime*1000L)
      .keyBy(_.payId)

    val processStream = payStream.intervalJoin(receiptStream)
      .between(Time.seconds(-15), Time.seconds(15))
      .process(new JoinMatch())

    processStream.print("interval join")
    env.execute()
  }

  class JoinMatch() extends ProcessJoinFunction[OrderPayEvent, ReceiptEvent, (OrderPayEvent, ReceiptEvent)]{
    override def processElement(in1: OrderPayEvent, in2: ReceiptEvent,
                                context: ProcessJoinFunction[OrderPayEvent, ReceiptEvent, (OrderPayEvent, ReceiptEvent)]
                                  #Context, out: Collector[(OrderPayEvent, ReceiptEvent)]): Unit = {
      out.collect(in1,in2)
    }
  }
}
