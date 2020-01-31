package com.river.flink.hotitems

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItemsHandler {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("C:\\River\\javacode\\RiverFlink\\HotItems\\src\\main\\resources\\UserBehavior.csv")
      .map( data=> {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toLong, dataArray(3).trim.toString, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val processStream = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new AverageAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process( new TopNHotItems(3))

    processStream.print()

    env.execute("Hot items")
  }

}

class CountAgg() extends AggregateFunction[UserBehavior, Long, Long]{
  override def createAccumulator(): Long = 0

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}

class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Long]{
  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1 + in.timestamp, acc._2 + 1)

  override def getResult(acc: (Long, Int)): Long = acc._1/acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1+acc1._1, acc._2+acc1._2)
}

class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect( ItemViewCount(key, window.getEnd, input.iterator.next()) )
  }
}

class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                              collector: Collector[String]): Unit = {
    itemState.add(value)
    context.timerService().registerEventTimeTimer(value.windowEnd+1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allitems: ListBuffer[ItemViewCount] = new ListBuffer()
    import scala.collection.JavaConversions._
    for(item <- itemState.get()) {
      allitems+=item
    }

    val sortedItems = allitems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    itemState.clear()

    val sb: StringBuilder = new StringBuilder();
    sb.append("时间：").append(new Timestamp( timestamp - 1 ) ).append("\n")

    for(i <- sortedItems.indices){
      val currentItem = sortedItems(i)
      sb.append("No").append(i + 1).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append("\n")
    }
    sb.append("========================")
    Thread.sleep(1000L)
    out.collect(sb.toString());
  }
}