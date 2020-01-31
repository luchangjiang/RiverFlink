package com.river.flink.pay

import java.util

import com.river.flink.login.LoginEvent
import com.river.flink.login.LoginHandler.getClass
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object PayTimeoutHandler {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val logPath = getClass.getResource("/OrderLog.csv")

    val dataStream = env.readTextFile(logPath.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderPayEvent(dataArray(0), dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignAscendingTimestamps(_.eventTime*1000L)
      .keyBy(_.orderId)

    val payPattern =Pattern.begin[OrderPayEvent]("begin").where(_.payStatus=="create")
      .followedBy("fellow").where(_.payStatus=="pay")
      .within(Time.minutes(15))

    val payStream = CEP.pattern(dataStream, payPattern)

    val outputTagStream: OutputTag[PayResult] = new OutputTag[PayResult]("payOutputTag")
    val resultStream = payStream.select(outputTagStream, new PayTimeoutMatch(), new PayMatch())

    resultStream.print("pay result")
    resultStream.getSideOutput(outputTagStream).print("side output result")

    env.execute("order pay")
  }

}

class PayMatch() extends PatternSelectFunction[OrderPayEvent, PayResult]{
  override def select(map: util.Map[String, util.List[OrderPayEvent]]): PayResult = {
    val orderId = map.get("fellow").iterator().next().orderId

    PayResult(orderId, "pay success")
  }
}

class PayTimeoutMatch() extends PatternTimeoutFunction[OrderPayEvent, PayResult]{
  override def timeout(map: util.Map[String, util.List[OrderPayEvent]], l: Long): PayResult = {
    val begin = map.get("begin").iterator().next()
    PayResult(begin.orderId, "pay timeout")
  }
}
