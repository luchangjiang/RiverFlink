package com.river.flink.hotitems

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis


object UvWithBloomHandler {
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
      .map(data => ("dummy", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UvCountByBloom())

    dataStream.print("uv count with bloom")
    env.execute()
  }
}

class MyTrigger() extends Trigger[(String, Long), TimeWindow]{
  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}
}

class Bloom(size: Long) extends Serializable {
  private val cap = if (size>0) size else 1<<27

  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for(i <- 0 until value.length){
      result = result*seed + value.charAt(i)
    }
    result & (cap-1)
  }
}

class UvCountByBloom() extends ProcessWindowFunction[(String, Long), UvViewCount, String, TimeWindow]{
//  lazy val jedis = new Jedis("192.168.0.102", 6379)
  lazy val jedis = new Jedis("localhost", 6379)
  lazy val bloom = new Bloom(1<<29)
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvViewCount]): Unit = {
    val storeField = context.window.getEnd.toString
    var count = 0L

    val countKey = "Uv#Count"
    val tmp = jedis.hget(countKey, storeField)
    if(tmp != null){
      count = tmp.toLong
    }

    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)
    val isExists = jedis.getbit(storeField, offset)
    if(!isExists){
      count = count + 1;
      jedis.hset(countKey, storeField, count.toString)
      jedis.setbit(storeField, offset, true)
    }
    out.collect(UvViewCount(storeField.toLong, count))
  }
}
