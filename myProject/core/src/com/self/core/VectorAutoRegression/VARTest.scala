package com.self.core.VectorAutoRegression

import java.sql.Timestamp

import com.self.core.VectorAutoRegression.utils.ToolsForTimeSeriesWarp
import com.self.core.baseApp.myAPP
import com.self.core.featurePretreatment.utils.Tools
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, NullableFunctions}

object VARTest extends myAPP {
  def createData(): DataFrame = {
    val timeParser = new java.text.SimpleDateFormat("yyyy-MM")

    val seq = List(
      ("1", "2017-08", 199, 0, 197),
      ("1", "2017-01", 123, -150, -150),
      ("1", "2017-02", 9, -150, -150),
      ("1", "2017-02", 19, -10, -150),
      ("1", "2017-03", 19, -150, -150),
      ("1", "2017-07", 1, 50, -150),
      ("1", "2017-05", 199, -8, -150),
      ("1", "2017-07", 120, 7, 0),
      ("1", "2017-09", -200, 100, 50),
      ("1", "2017-10", 60, 50, 0),
      ("1", "2017-11", 199, 98, 0),
      ("1", "2017-12", 100, 0, 10),
      ("2", "2017-01", 99, -70, -150),
      //      ("2", null, 199, -150, -150),
      ("2", "2018-02", 1, -50, -15),
      ("2", "2018-05", 90, -0, -1),
      ("2", "2018-06", 19, -50, -15),
      //      ("2", null, 199, -150, -150),
      ("2", "2018-03", 1, -50, -15),
      ("2", "2018-07", 90, -0, -1),
      ("2", "2018-08", 19, -50, -15),
      ("2", "2018-09", 1, -50, -15),
      ("2", "2018-10", 90, -0, -1),
      ("2", "2018-11", 19, -50, -15)
    ).map(tup => (tup._1, new java.sql.Timestamp(timeParser.parse(tup._2).getTime), tup._3, tup._4, tup._5))


    sqlc.createDataFrame(seq).toDF("id", "dayTime", "x", "y", "z")
  }


  override def run(): Unit = {
    println("This is a test file.")

    /** 0)获得数据和一些初始信息 */
    val rawDataFrame: DataFrame = createData()
    val sQLContext = rawDataFrame.sqlContext
    val sparkContext = sQLContext.sparkContext

    val timeColName: String = "dayTime" // @todo 还需要一个将时间根据窗口和相位规则进行平滑和补全的工具函数

    // 数据只能为Numeric类型
    val variablesColNames = Array("x", "y", "z")
    // 列类型判断
    variablesColNames.foreach {
      name =>
        Tools.columnTypesIn(name, rawDataFrame, true, StringType, DoubleType, IntegerType, LongType, FloatType, ByteType)
    }

    val rawDataDF = rawDataFrame.na.drop(variablesColNames) // 只要有一个变量为空就会drop

    require(rawDataDF.count() > 0, "您的数据根据变量去除缺失值数目为0，由于变量的null值太多，算子无法根据推测补全。" +
      "因此建议您先对数据进行审查。")


    /** 1)分箱获得时间序列窗口的id */
    /** 时间序列起止时间和窗宽信息 */
    val (startTime: Long, endTime: Long) = {
      val timeParser = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      (timeParser.parse("2017-01-01 00:00:00").getTime,
        timeParser.parse("2019-01-01 00:00:00").getTime)
    }

    val windowUnit: String = "month"
    val windowLength: Int = try {
      "2".toDouble.toInt
    } catch {
      case e: Exception => throw new Exception(s"您输入的时间序列窗宽长度不能转为数值类型, 具体信息为${e.getMessage}")
    }
    require(windowLength > 0, "您输入的时间序列窗宽长度需要大于0")

    /** 确认start和end恰好是整数个窗宽 */
    val binningEndTime = ToolsForTimeSeriesWarp.binning(
      new Timestamp(endTime), startTime, windowLength, windowUnit)
    require(binningEndTime.getTime == endTime, s"要求您输入的起止时间应该恰好是您输入的窗宽的整数倍：" +
      s"您输入的起始时间是${new Timestamp(startTime)}, 窗宽是${windowLength + windowUnit}, " +
      s"此时时间序列接近您输入的截止时间应为$binningEndTime")

    /** 将窗宽分箱获得 => 时间 + 箱子所在id */
    val binningIdColName: String = Tools.nameANewCol("windowId", rawDataDF)

    val binningUDF = NullableFunctions.udf(
      (time: Timestamp) => ToolsForTimeSeriesWarp.binningForId(time, startTime, windowLength, windowUnit)
    )

    val DFAfterBinning = rawDataDF.withColumn(binningIdColName, binningUDF(col(timeColName)))
    println("第一步分箱后结果为：")
    DFAfterBinning.show()

    /** 2)每个窗口进行规约 --规约方式可以为最大、最小、均值、最早四种方式，如果一个窗口内某个变量全为null值则规约为Double.NaN值 */
    // 规约方式
    val reduction = "first" // "min", "mean", "first"
    import org.apache.spark.sql.functions.{first, max, mean, min}
    val DFAfterReduction = reduction match {
      case "max" =>
        DFAfterBinning.groupBy(col(binningIdColName))
          .agg(max(col(variablesColNames.head)).cast(DoubleType).alias(variablesColNames.head),
            variablesColNames.drop(1).map(name => max(col("`" + name + "`")).cast(DoubleType).alias(name)): _*)

      case "min" =>
        DFAfterBinning.groupBy(col(binningIdColName))
          .agg(min(col(variablesColNames.head)).cast(DoubleType).alias(variablesColNames.head),
            variablesColNames.drop(1).map(name => min(col("`" + name + "`")).cast(DoubleType).alias(name)): _*)

      case "mean" =>
        DFAfterBinning.groupBy(col(binningIdColName))
          .agg(mean(col(variablesColNames.head)).cast(DoubleType).alias(variablesColNames.head),
            variablesColNames.drop(1).map(name => mean(col("`" + name + "`")).cast(DoubleType).alias(name)): _*)

      case "first" =>
        DFAfterBinning.sort(col(timeColName)).groupBy(col(binningIdColName))
          .agg(first(col(variablesColNames.head)).cast(DoubleType).alias(variablesColNames.head),
            variablesColNames.drop(1).map(name => first(col("`" + name + "`")).cast(DoubleType).alias(name)): _*)
      // 验证一下集群上是不是这样
    }


    println("第二步按窗口规约后结果为：")
    DFAfterReduction.show()


    /** 3)窗口补全和记录补全 */
    // 补全方式 "mean", "zero", "linear"


//    def windowComplement(startWindowId: Long, endWindowId: Long) = {
//      /** 1)根据窗口id生成PairRDD并相机补全最大最小值 */
//      var rdd: RDD[(Long, Row)] = DFAfterReduction.rdd.map {
//        row =>
//          (row.getAs[Long](binningIdColName), row)
//      }
//
//      val (minWindowId, minWindowValue) = rdd.max()(Ordering.by[(Long, Row), Long](_._1))
//      val (maxWindowId, maxWindowValue) = rdd.min()(Ordering.by[(Long, Row), Long](_._1))
//      if (minWindowId != startWindowId) {
//        rdd = rdd.union(sparkContext.parallelize(Seq((startWindowId, minWindowValue))))
//      } // 如果开头缺失则找它最近时间的值补全（线性插值时这样）
//
//      if (maxWindowId != endWindowId) {
//        rdd = rdd.union(sparkContext.parallelize(Seq((endWindowId, maxWindowValue))))
//      } // 如果结尾缺失则找它最近时间的值补全（线性插值时这样）
//
//      /** 2)获得非空分区的数目 */
//      val numParts = rdd.partitions.length
//
//      /** 3)根据窗口id进行重分区 */
//      class PartitionByWindowId(numParts: Int) extends Partitioner {
//        override def numPartitions: Int = numParts
//
//        override def getPartition(key: Any): Int =
//          key match {
//            case i: Long =>
//              (i / numParts).toInt // 必定大于等于0
//            case _: Exception =>
//              throw new SparkException("key的类型不是long")
//          }
//
//      }
//      val rddPartitionByWinId: RDD[(Long, Row)] = rdd.partitionBy(new PartitionByWindowId(numParts))
//
//      /** 4)定义分区方式 windowId / numParts, 如果 */
//      rddPartitionByWinId.mapPartitionsWithIndex {
//        (partitionId, iter) =>
//          if (partitionId != numParts - 1 && partitionId != 0) { // 将第一个数据备双份
//            val partitionNum = (partitionId.toLong + 1) << 33
//            var i = 0
//            val result = mutable.ArrayBuilder.make[(Long, Row)]()
//            iter.foreach {
//              case (windowId, value) => {
//                i += 1
//                if (i == 1) {
//                  result += Tuple2(partitionNum - 1, value)
//                  result += Tuple2(partitionNum, value)
//                } else {
//                  result += Tuple2(partitionNum + i - 1, value)
//                }
//              }
//            }
//            val mm: Iterator[(Long, Row)] = result.result().toIterator
//            mm
//          } else if (partitionId != 0) { // 如果是第一个分区此时不用第一个元素双份，但是如果没有windowId = minWindowId应该加上
//            new Iterator
//          } else { // 最后一个分区，第一个元素双份，如果没有windowId =
//            new Iterator
//          }
//
//
//      }
//
//
//      class OverLapPartitioner(numParts: Int) extends Partitioner {
//        override def numPartitions: Int = numParts
//
//        override def getPartition(key: Any): Int = {
//          val id = key match {
//            case i: Long =>
//              (i >> 33) - 1
//            case _: Exception =>
//              throw new SparkException("key的类型不是long")
//          }
//          val modNum = (id % numParts).toInt
//          if (modNum < 0) modNum + numParts else modNum
//        }
//      }
//
//      val rePartitionRdd = partitionMaterial.partitionBy(new OverLapPartitioner(4))
//      val sum = rePartitionRdd.map(_._2).mapPartitions(
//        iter => {
//          var result = 0.0
//          var lastValue = 1.0
//          while (iter.hasNext) {
//            val value = iter.next()
//            result += value * lastValue
//            lastValue = value
//          }
//          Array(result).toIterator
//        }
//      ).reduce(_ + _)
//      println("最终结果:" + sum)
//
//      val endTime1 = System.nanoTime()
//      val costTime1 = (endTime1 - startTime1) / 1000
//      println(s"花费时间:${costTime1}毫秒")
//      costTime1
//    }


  }
}
