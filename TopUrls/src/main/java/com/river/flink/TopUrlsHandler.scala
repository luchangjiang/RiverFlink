package com.river.flink

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


case class ApacheLogEvent(ip: String, timestamp: Long, method: String, url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object TopUrlsHandler {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("C:\\River\\javacode\\RiverFlink\\TopUrls\\src\\main\\resources\\apache.log")
      .map( data => {
        val dataArray = data.split(" ")
        val sdf: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = sdf.parse(dataArray(3).trim).getTime
        ApacheLogEvent(dataArray(0).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
      })
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(5)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.timestamp
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.seconds(60))
      .aggregate( new CountAgg(), new WindowResult())

    val processedStream = dataStream
      .keyBy(_.windowEnd)
      .process( new TopNHotUrls(5) )

    processedStream.print("hot urls")

    env.execute()
  }

}

class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}

class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{

  lazy val mapState: MapState[String, Long] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Long]("map-state", classOf[String], classOf[Long]))

  override def processElement(value: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                              collector: Collector[String]): Unit = {
    mapState.put(value.url, value.count)
    context.timerService().registerEventTimeTimer(value.windowEnd+1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]
    #OnTimerContext, out: Collector[String]): Unit = {
    val allUrls: ListBuffer[(String, Long)] = new ListBuffer[(String, Long)]

    val iter = mapState.entries().iterator()
    while(iter.hasNext){
      val entry = iter.next()
      allUrls +=((entry.getKey, entry.getValue))
    }

    mapState.clear()

    val sortedUrls = allUrls.sortWith(_._2>_._2).take(topSize)

    val result: StringBuilder = new StringBuilder();
    result.append("时间：").append(new Timestamp(timestamp-1)).append("\n")
    for(i<-sortedUrls.indices){
      val currentItem = sortedUrls(i)
      result.append("No").append(i+1).append(":")
        .append(" url=").append(currentItem._1)
        .append(" 浏览量=").append(currentItem._2)
        .append("\n")
    }
    result.append("============================")
//    Thread.sleep(1000)
    out.collect(result.toString())
  }
}