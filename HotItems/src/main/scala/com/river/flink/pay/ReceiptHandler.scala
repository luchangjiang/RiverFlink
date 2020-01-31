package com.river.flink.pay

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ReceiptHandler {
  val payOutput : OutputTag[OrderPayEvent] = new OutputTag[OrderPayEvent]("pay-output")
  val receiptOutput : OutputTag[ReceiptEvent] = new OutputTag[ReceiptEvent]("receipt-output")
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

    val coMapStream = payStream.connect(receiptStream)
      .process(new MatchProcess())

    coMapStream.print()
    coMapStream.getSideOutput(payOutput).print("pay side output")
    coMapStream.getSideOutput(receiptOutput).print("receipt side output")

    env.execute()
  }

  class MatchProcess() extends CoProcessFunction[OrderPayEvent, ReceiptEvent, (OrderPayEvent, ReceiptEvent)]{
    lazy val payState: ValueState[OrderPayEvent] = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderPayEvent]("pay-state", classOf[OrderPayEvent]))

    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(
      new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))

    override def processElement1(pay: OrderPayEvent, context: CoProcessFunction[OrderPayEvent, ReceiptEvent,
      (OrderPayEvent, ReceiptEvent)]#Context, out: Collector[(OrderPayEvent, ReceiptEvent)]): Unit = {

      if(receiptState.value()!=null){
        out.collect(pay, receiptState.value())
        receiptState.clear()
      }
      else{
        payState.update(pay)
        context.timerService().registerEventTimeTimer(pay.eventTime*1000*5)
      }
    }

    override def processElement2(receipt: ReceiptEvent, context: CoProcessFunction[OrderPayEvent, ReceiptEvent,
      (OrderPayEvent, ReceiptEvent)]#Context, out: Collector[(OrderPayEvent, ReceiptEvent)]): Unit = {
      if(payState.value()!=null){
        out.collect(payState.value(), receipt)
        payState.clear()
      }
      else{
        receiptState.update(receipt)
        context.timerService().registerEventTimeTimer(receipt.eventTime*1000*5)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderPayEvent, ReceiptEvent,
      (OrderPayEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderPayEvent, ReceiptEvent)]): Unit = {

      if(payState.value()!=null){
        ctx.output(payOutput, payState.value())
      }

      if(receiptState.value()!=null){
        ctx.output(receiptOutput, receiptState.value())
      }

      payState.clear()
      receiptState.clear()
    }
  }

}
