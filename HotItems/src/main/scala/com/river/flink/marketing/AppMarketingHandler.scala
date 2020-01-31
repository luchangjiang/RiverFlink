package com.river.flink.marketing

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.river.flink.hotitems.WindowResult
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

object AppMarketingHandler {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource( new SimulateEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data => ("dummy",1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .aggregate(new CountAgg(), new WindowResult())

    dataStream.print()
    env.execute("marketing count by channel")
  }
}

class SimulateEventSource() extends RichSourceFunction[MarketingUserBehavior]{
  var running = true

  val behaviorTypes: Seq[String] = Seq[String]("CLICK","DOWNLOAD","INSTALL","UNINSTALL")
  val channelSet: Seq[String] = Seq[String]("HUAWEI","XIAOMI","OPPO","APPLE","SANXING")
  val rand = new Random()
  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    val maxElements = Long.MaxValue
    var count = 0L
    while(running && count<maxElements){
      val userId = UUID.randomUUID().toString
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val timestamp = System.currentTimeMillis()

      ctx.collect(MarketingUserBehavior(userId, behavior, channel, timestamp))
      count = count+1
      TimeUnit.MICROSECONDS.sleep(10L)
    }

  }

  override def cancel(): Unit = {
    running = false
  }
}

class CountAgg() extends AggregateFunction[(String, Long), Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: (String, Long), acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}

class WindowResult() extends WindowFunction[Long, MarketingViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]):
  Unit = {
    out.collect(MarketingViewCount(window.getStart.toString, window.getEnd.toString, input.iterator.next()))
  }
}
