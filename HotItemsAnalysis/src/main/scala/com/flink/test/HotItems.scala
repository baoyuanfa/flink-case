package com.flink.test

import com.flink.utils.MyKafkaUtil
import com.sun.jmx.snmp.Timestamp
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)


object HotItems {
    def main(args: Array[String]): Unit = {
        //创建streamEnv
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //定义EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        import org.apache.flink.api.scala._
        //读取文件
        val stream = env
                //.readTextFile("F:\\JAVA\\SGGworkspace\\userBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
                .addSource(MyKafkaUtil.getConsumer("HotItmes"))
                //转换格式为UserBehavior
                .map(line => {
            val fields: Array[String] = line.split(",")
            UserBehavior(fields(0).toLong, fields(1).toLong, fields(2).toInt, fields(3), fields(4).toLong)
        })
                //指定时间戳和watermark
                .assignAscendingTimestamps(_.timestamp * 1000)
                .filter(_.behavior == "pv")
                .keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResultFunction())
                .keyBy("windowEnd")
                .process( new TopNHotItems(3) )
                .print()

        //执行
        env.execute(this.getClass.getSimpleName)


    }
    //自定义实现聚合函数
    class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
        override def createAccumulator(): Long = 0L

        override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a + b
    }

    //自定义是实现windowFunction
    class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {

            val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0

            val count: Long = input.iterator.next()

            out.collect(ItemViewCount(itemId, window.getEnd, count))
        }
    }

    //自定义实现processFunction
    class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String]{

        //定义状态ListState
        private var itemState : ListState[ItemViewCount] = _


        override def open(parameters: Configuration): Unit = {
            super.open(parameters)
            //命名状态变量的名字和类型
            val itemStateDec = new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount])
            itemState = getRuntimeContext.getListState(itemStateDec)
        }

        override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
            // 每条数据都保存到状态中
            itemState.add(value)

            // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
            // 也就是当程序看到windowend + 1的水位线watermark时，触发onTimer回调函数
            ctx.timerService().registerEventTimeTimer( value.windowEnd + 1 )
        }

        //定时器触发操作，从state中取出数据，排序后输出
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
            //获取所有的商品点击信息
            val allItems : ListBuffer[ItemViewCount] = ListBuffer()

            import scala.collection.JavaConversions._

            for (item <- itemState.get) {
                allItems += item
            }
            // 提前清除状态中的数据，释放空间
            itemState.clear()

            //按照点击量进行排序
            val sortedItems = allItems.sortBy(_.count)(Ordering[Long].reverse).take(topSize)

            //将排序数据格式化，便于输出
            val result: StringBuilder = new StringBuilder
            result.append("====================================\n")
            result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

            for(i <- sortedItems.indices){
                val currentItem: ItemViewCount = sortedItems(i)
                // e.g.  No1：  商品ID=12224  浏览量=2413
                result.append("No").append(i+1).append(":")
                        .append("  商品ID=").append(currentItem.itemId)
                        .append("  浏览量=").append(currentItem.count).append("\n")
            }
            result.append("====================================\n\n")
            // 控制输出频率，模拟实时滚动结果
            Thread.sleep(1000)
            out.collect(result.toString)

        }
    }
}
