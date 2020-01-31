package com.river.flink.marketing


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AppMarketingChannelHandler {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource( new SimulateEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data => ((data.behavior, data.channel),1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .process(new MarketingCountByChannelWindow())

    dataStream.print()
    env.execute("marketing count by channel")
  }
}

class MarketingCountByChannelWindow() extends ProcessWindowFunction[((String, String),Long),
  MarketingViewChannelCount, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)],
                       out: Collector[MarketingViewChannelCount]): Unit = {
    out.collect(MarketingViewChannelCount(context.window.getStart.toString, context.window.getEnd.toString,
      key._1, key._2, elements.size))
  }
}
