package DataStreamDemo

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment


import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object KeyByDemo {

  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    // 输入: 用户行为。某个用户在某个时刻点击或浏览了某个商品，以及商品的价格。
    val array = Array(people("userID1", 1293984000, "click", "productID1", 10),people("userID2", 1293984001, "browse", "productID2", 8),people("userID1", 1293984002, "click", "productID1", 10))
    val fromArray: DataStream[String] = environment.fromCollection(array)
    // 转换: 按指定的Key(这里,用户ID)对数据重分区，将相同Key(用户ID)的数据分到同一个分区
    val result = source.keyBy()
    // 输出: 输出到控制台
    //3> UserAction(userID=userID1, eventTime=1293984000, eventType=click, productID=productID1, productPrice=10)
    //3> UserAction(userID=userID1, eventTime=1293984002, eventType=click, productID=productID1, productPrice=10)
    //2> UserAction(userID=userID2, eventTime=1293984001, eventType=browse, productID=productID2, productPrice=8)
    result.print.setParallelism(3)
    env.execute
  }
  case class people(userID:String,eventTime:Int,eventType:String,productID:String,productPrice:Int)
}
