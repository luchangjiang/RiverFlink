package com.river.flink.login

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginHandler {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val logPath = getClass.getResource("/LoginLog.csv")

    val dataStream = env.readTextFile(logPath.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignAscendingTimestamps(_.eventTime*1000L)

    val loginPattern =Pattern.begin[LoginEvent]("begin").where(_.eventType=="fail")
      .next("next").where(_.eventType=="fail")
      .within(Time.seconds(2))

    val patternStream = CEP.pattern(dataStream, loginPattern)

    val loginFailStream = patternStream.select( new LoginFailFunction())

    loginFailStream.print("login fail")
    env.execute()
  }

}

class LoginFailFunction() extends PatternSelectFunction[LoginEvent, LoginFailedView]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginFailedView = {
    val firstFail = map.get("begin").iterator().next()
    val nextFail = map.get("next").iterator().next()
    LoginFailedView(firstFail.userId, firstFail.eventTime, nextFail.eventTime, "two time next fail to login")
  }
}
