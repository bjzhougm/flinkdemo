package test

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SocketWindowWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val textStream = env.socketTextStream("localhost", 9000, '\n')
    val windowWordCount = textStream
      .flatMap(line => line.split("\\s"))
      .map(word => (word, 1))
      .keyBy(a=>(a._2,a._1))
      .timeWindow(Time.seconds(5))
      .sum(1)
    windowWordCount.print().setParallelism(1)
    env.execute("Socket Window WordCount")
  }
}
