package com.flink.test

import java.lang
import java.text.SimpleDateFormat

import com.sun.jmx.snmp.Timestamp
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)


object NetWorkTraffic {
    def main(args: Array[String]): Unit = {
        //创建streamEnv
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //设置eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)


        env
        //读取文件
        .readTextFile("F:\\JAVA\\SGGworkspace\\userBehaviorAnalysis\\NetWorkTrafficAnalysis\\src\\main\\resources\\apachetest.log")
        .map( line => {
            //拆分一行数据
            val fields: Array[String] = line.split(" ")
            //转换时间为long
            val sdft = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
            val timestamp = sdft.parse(fields(3)).getTime
            //转换为ApacheLogEvent
            ApacheLogEvent(fields(0), fields(2), timestamp, fields(5), fields(6))
        })
        //乱序时间戳，创建时间戳和水位
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(10)) {
            override def extractTimestamp(element: ApacheLogEvent): Long = {
                element.eventTime
            }
        })
        //过滤出Get方法的数据
        .filter(_.method == "GET")
        //根据url分流
        //.keyBy("url")
        .keyBy(_.url)
        //开窗
        .timeWindow(Time.minutes(1), Time.seconds(5))
        //窗口内聚合
        .aggregate(new CountAgg(), new WindowResultFunction())
        //窗口聚合后进行分流
        .keyBy(_.windowEnd)
        //自定义规则
        .process(new TopNHotUrls(5))
        .print()

        //执行程序
        env.execute(this.getClass.getSimpleName)
    }

    //自定义窗口预聚和
    class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long]{
        override def createAccumulator(): Long = 0L

        override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a + b
    }

    //自定义WindowFunction
    class WindowResultFunction extends WindowFunction[Long, UrlViewCount, String, TimeWindow]{
        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
            //获取url
            val url : String = key
            //获取count
            val count: Long = input.iterator.next()
            //获取windowEnd
            val windowEnd: Long = window.getEnd + 1
            //返回结果
            out.collect(UrlViewCount(url, windowEnd, count))
        }
    }

    //自定义processFunction, 统计访问量最大的url，并排序
    class TopNHotUrls(topSize : Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

        //定义状态变量的名字和类型
        lazy val url : ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]( "urlState", classOf[UrlViewCount] ))

        override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
            //每条数据都保存到状态中
            url.add(value)
            //注册一个定时器
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 10 * 1000)


        }

        //实现onTimer
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
            // 从状态中获取所有的url访问量
            val allUrlViews : ListBuffer[UrlViewCount] = new ListBuffer()

            import scala.collection.JavaConversions._
            //获取状态中的所有值
            for (urlView <- url.get()) {
                allUrlViews += urlView
            }

            //清空state
            url.clear()

            //按照访问量排序输出
            val sortedUrlViews: ListBuffer[UrlViewCount] = allUrlViews.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

            // 将排名信息格式化成 String, 便于打印
            var result: StringBuilder = new StringBuilder
            result.append("====================================\n")
            result.append("时间: ").append(new Timestamp(timestamp - 10 * 1000)).append("\n")

            for (i <- sortedUrlViews.indices) {
                val currentItem: UrlViewCount = sortedUrlViews(i)
                // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
                result.append("No").append(i+1).append(":")
                        .append("  URL=").append(currentItem.url)
                        .append("  流量=").append(currentItem.count).append("\n")
            }
            result.append("====================================\n\n")
            // 控制输出频率，模拟实时滚动结果
            Thread.sleep(1000)
            out.collect(result.toString)

        }
    }

}
