package com.river.flink.ad

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AdBlackHandler {
  val blackListOutput: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val logPath = getClass.getResource("/AdClickLog.csv")

    val dataStream = env.readTextFile(logPath.getPath)
      .map(data => {
        val dataArray = data.split(",")
        AdClickEvent(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp*1000L)

    val filterBlackListStream = dataStream
      .keyBy(data => (data.userId, data.adId))
      .process( new processBlack(100))

    val processStream = filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1))
      .aggregate(new CountAgg(), new WindowResult())

    processStream.print("process count")
    filterBlackListStream.getSideOutput(blackListOutput).print("blackList")
    env.execute("province count")
  }

  class processBlack(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]{
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("current-count", classOf[Long]))
    lazy val sendFlag: ValueState[Boolean]= getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("send-flag", classOf[Boolean]))
    lazy val dayMills = 1000*60*60*24
    lazy val resetTime: ValueState[Long]= getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-time", classOf[Long]))
    override def processElement(input: AdClickEvent, context: KeyedProcessFunction[(Long, Long),
      AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {

      val currCount = countState.value()
      if(currCount>=maxCount) {
        if (!sendFlag.value()) {
          sendFlag.update(true)
          context.output(blackListOutput, BlackListWarning(input.userId, input.adId, "Input blacklist"))

          val ts = (context.timerService().currentProcessingTime() / dayMills + 1) * dayMills
          resetTime.update(ts)
          context.timerService().registerEventTimeTimer(ts)
        }
        return ;
      }
      countState.update(currCount + 1)
      out.collect(input)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]
      #OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      if(timestamp == resetTime.value()) {
        sendFlag.clear()
        resetTime.clear()
        countState.clear()
      }
    }
  }
}

class CountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickEvent, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}

class WindowResult() extends WindowFunction[Long, ProvinceViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[ProvinceViewCount]): Unit = {
    out.collect(ProvinceViewCount(window.getEnd, key, input.iterator.next()))
  }
}