package DataStreamDemo

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * https://blog.csdn.net/qq_21383435/article/details/105943989
 */

object ProcessFunctionScala {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = env.socketTextStream("localhost", 9999)
    val typeAndData: DataStream[(String, String)] = stream.map(x => (x.split(",")(0), x.split(",")(1))).setParallelism(4)
    typeAndData.keyBy(0).process(new MyProcessFunction()).print("结果")
    env.execute()
  }

  /**
   * 实现：
   *    根据key分类，统计每个key进来的数据量，定期统计数量，如果数量为0则预警
   */
  class MyProcessFunction extends  KeyedProcessFunction[Tuple,(String,String),String]{

    //统计间隔时间
    val delayTime : Long = 1000 * 10

    lazy val state : ValueState[(String,Long)] = getRuntimeContext.getState[(String,Long)](new ValueStateDescriptor[(String, Long)]("cjCount",classOf[(String, Long)]))

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, (String, String), String]#OnTimerContext, out: Collector[String]): Unit = {

      printf("定时器触发,时间为：%d,状态为：%s,key为：%s\n",timestamp,state.value(),ctx.getCurrentKey)
      if(state.value()._2==0){
        //该时间段数据为0,进行预警
        printf("类型为:%s,数据为0,预警\n",state.value()._1)
      }
      //定期数据统计完成后，清零
      state.update(state.value()._1,0)
      //再次注册定时器执行
      val currentTime: Long = ctx.timerService().currentProcessingTime()
      ctx.timerService().registerProcessingTimeTimer(currentTime + delayTime)
    }

    override def processElement(value: (String, String), ctx: KeyedProcessFunction[Tuple, (String, String), String]#Context, out: Collector[String]): Unit = {
      printf("状态值：%s,state是否为空：%s\n",state.value(), state.value()==null)
      if(state.value() == null){
        //获取时间
        val currentTime: Long = ctx.timerService().currentProcessingTime()
        //注册定时器十秒后触发
        ctx.timerService().registerProcessingTimeTimer(currentTime + delayTime)
        printf("定时器注册时间：%d\n",currentTime+10000L)
        state.update(value._1,value._2.toInt)
      } else{
        //统计数据
        val key: String = state.value()._1
        var count: Long = state.value()._2
        count += value._2.toInt
        //更新state值
        state.update((key,count))
      }

      println(getRuntimeContext.getTaskNameWithSubtasks+"->"+value)
      printf("状态值：%s\n",state.value())
      //返回处理后结果
      out.collect("处理后返回数据->"+value)
    }
  }
}
