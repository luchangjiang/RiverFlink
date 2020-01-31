package com.river.flink.hotitems

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UserViewHandler {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime )

    val resource = getClass.getResource("/UserBehavior.csv")

    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toLong, dataArray(3).trim.toString, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp*1000L)
      .filter(_.behavior == "pv")
        .timeWindowAll(Time.hours(1))
        .apply( new UvCountByWindow())

      dataStream.print("uv count")
      env.execute()
    }
}

class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvViewCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvViewCount]): Unit = {
    var uvSet = Set[Long]()
    for(userBehavior <- input){
      uvSet += userBehavior.userId
    }

    out.collect(UvViewCount(window.getEnd, uvSet.size))
  }
}
