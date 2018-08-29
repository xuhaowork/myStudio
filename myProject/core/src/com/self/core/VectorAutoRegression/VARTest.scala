package com.self.core.VectorAutoRegression

import java.sql.Timestamp

import scala.collection.mutable
import com.self.core.VectorAutoRegression.utils.ToolsForTimeSeriesWarp
import com.self.core.baseApp.myAPP
import com.self.core.featurePretreatment.utils.Tools
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, NullableFunctions, Row}
import org.apache.spark.{Partitioner, SparkException}

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


    def windowComplement(startWindowId: Long, endWindowId: Long) = {
      /** 1)根据窗口id生成PairRDD并相机补全最大最小值 */
      var rdd: RDD[(Long, Row)] = DFAfterReduction.rdd.map {
        row =>
          (row.getAs[Long](binningIdColName), Row.fromSeq(row.toSeq.drop(1)))
      }

      val (minWindowId, minWindowValue) = rdd.min()(Ordering.by[(Long, Row), Long](_._1))
      val (maxWindowId, maxWindowValue) = rdd.max()(Ordering.by[(Long, Row), Long](_._1))
      if (minWindowId != startWindowId) {
        println("minWindowId", minWindowId, startWindowId)
        rdd = rdd.union(sparkContext.parallelize(Seq((startWindowId, minWindowValue))))
      } // 如果开头缺失则找它最近时间的值补全（线性插值时这样）

      if (maxWindowId != endWindowId) {
        rdd = rdd.union(sparkContext.parallelize(Seq((endWindowId, maxWindowValue))))
      } // 如果结尾缺失则找它最近时间的值补全（线性插值时这样）

      /** 2)获得非空分区的数目 */
      val numParts = 4
      //        rdd.partitions.length

      /** 3)根据窗口id进行重分区 */
      class PartitionByWindowId(numParts: Int) extends Partitioner {
        override def numPartitions: Int = numParts

        override def getPartition(key: Any): Int =
          key match {
            case i: Long =>
              (i / numParts).toInt // 必定大于等于0
            case _: Exception =>
              throw new SparkException("key的类型不是long")
          }

      }
      val rddPartitionByWinId: RDD[(Long, Row)] = rdd.repartitionAndSortWithinPartitions(
        new PartitionByWindowId(numParts)
      ) // 确保分区后的顺序

      println("partition前")
      rddPartitionByWinId.mapPartitionsWithIndex {
        case (index, iter) =>
          iter.map(v => (index, v))
      }.collect().sortBy(_._1).foreach(println)

      /** 4)定义分区 */
      class OverLapPartitioner(numParts: Int) extends Partitioner {
        override def numPartitions: Int = numParts

        override def getPartition(key: Any): Int = {
          val id = key match {
            case i: Long =>
              i >> 33
            case _: Exception =>
              throw new SparkException("key的类型不是long")
          }
          val modNum = (id % numParts).toInt
          if (modNum < 0) modNum + numParts else modNum
        }
      }

      val rePartitionRdd: RDD[(Long, (Long, Row))] = rddPartitionByWinId.mapPartitionsWithIndex {
        (partitionId, iter) =>
          val partitionNum = partitionId.toLong << 33
          var i = 0
          val result = mutable.ArrayBuilder.make[(Long, (Long, Row))]()
          iter.foreach {
            value =>
              i += 1
              if (partitionId != 0 && i == 1) {
                result += Tuple2(partitionNum - 1, value)
                result += Tuple2(partitionNum, value)
              } else {
                result += Tuple2(partitionNum + i - 1, value)
              }

          }
          result.result().toIterator
      }.partitionBy(new OverLapPartitioner(numParts))

      println("partition后")
      rePartitionRdd.mapPartitionsWithIndex {
        case (index, iter) =>
          iter.map(v => (index, v))
      }.collect().sortBy(_._1).foreach(println)



      //      /** 按起止id补全 */
      //      val resultRdd = rePartitionRdd.mapPartitions(
      //        iter => {
      //          val result = mutable.ArrayBuilder.make[(Long, (Long, Row))]()
      //
      //          var lastWindowId = 0L
      //          var plus = 0L // 循环计数器
      //          var lastRow: Row = null
      //          var i = 0
      //          var lastKey = 0L
      //          while (iter.hasNext){
      //            val (key, (windowId, row)) = iter.next()
      //
      //            if(i == 0) {
      //              result += Tuple2(key + plus, (windowId, row))
      //              lastWindowId = windowId
      //              lastKey = key
      //              lastRow = row
      //              plus += 1
      //            } else {
      //              for (each <- 1L to (windowId - lastWindowId)) {
      //                result += Tuple2(lastKey + plus, (lastWindowId + each, fillValue(lastRow, row)))
      //                //          lastKey += 1
      //                plus += 1
      //              }
      //
      //              lastWindowId = windowId
      //              lastRow = row
      //            }
      //
      //            i += 1
      //          }
      //
      //          result.result().dropRight(1).toIterator
      //        }
      //      )
      //
      //      resultRdd
    }

    windowComplement(0, 14)




    //    def fillValue(starRow: Row, endRow: Row): Row = {
    //      Row.fromSeq(starRow.toSeq.zip(endRow.toSeq).map { case (v1, v2) => v1.asInstanceOf[Double] + v2.asInstanceOf[Double]})
    //    }
    //
    //    val u = windowComplement(0, 14)
    //
    //    u.mapPartitionsWithIndex{
    //      case (index, iter) =>
    //        iter.map(v => (index, v))
    //    }.collect().sortBy(_._1).foreach(println)


    //    (0,(8589934594,  (6,[200.0,48.0,-15.0])))
    //    (0,(8589934595,  (7,[2.0,-100.0,-30.0])))
    //    (0,(8589934592,  (4,[-200.0,100.0,50.0])))
    //    (0,(8589934593,  (5,[-1.0,198.0,50.0])))
    //
    //    (1,(17179869187, (11,[20.0,-100.0,-30.0])))
    //    (1,(17179869188, (12,[142.0,-200.0,-165.0])))
    //    (1,(17179869188, (12,[142.0,-200.0,-165.0])))
    //    (1,(17179869188, (12,[142.0,-200.0,-165.0])))
    //    (1,(17179869189, (13,[142.0,-200.0,-165.0])))
    //    (1,(17179869184, (8,[90.0,0.0,-1.0])))
    //    (1,(17179869185, (9,[180.0,0.0,-2.0])))
    //    (1,(17179869186, (10,[91.0,-50.0,-16.0])))
    //
    //    (3,(0,           (0,[123.0,-150.0,-150.0])))
    //    (3,(1,           (1,[38.0,-200.0,-165.0])))
    //    (3,(2,           (2,[218.0,-158.0,-300.0])))
    //    (3,(3,           (3,[200.0,42.0,-300.0])))

    // repartition是无序还是foreach println是无序

  }



}
