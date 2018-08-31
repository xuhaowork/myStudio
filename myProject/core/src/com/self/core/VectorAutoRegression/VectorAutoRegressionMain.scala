package com.self.core.VectorAutoRegression

import java.sql.Timestamp

import com.google.gson.JsonParser
import com.self.core.VectorAutoRegression.utils.ToolsForTimeSeriesWarp
import com.self.core.baseApp.myAPP
import com.self.core.featurePretreatment.utils.Tools
import org.apache.spark.{Partitioner, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, NullableFunctions, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable


object VectorAutoRegressionMain extends myAPP{
  def createData(): Unit = {
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
      ("2", "2017-01", 99, -70, -150),
      ("2", "2018-02", 1, -50, -15),
      ("2", "2018-07", 90, -0, -1),
      ("2", "2018-08", 19, -50, -15),
      ("2", "2018-09", 1, -50, -15),
      ("2", "2018-10", 90, -0, -1),
      ("2", "2018-11", 19, -50, -15)
    ).map(tup => (tup._1, new java.sql.Timestamp(timeParser.parse(tup._2).getTime), tup._3, tup._4, tup._5))

    val newDataFrame = sqlc.createDataFrame(seq).toDF("id", "dayTime", "x", "y", "z")

    val rddTableName = "模拟时间序列_XFdCFEE5"
    newDataFrame.registerTempTable(rddTableName)
    newDataFrame.sqlContext.cacheTable(rddTableName)
    outputrdd.put(rddTableName, newDataFrame)
  }


  def testTimeSeriesWarping(): Unit = {
    createData()

    /** 0)获得数据和一些初始信息 */
    val jsonparam = """{"RERUNNING":{"nodeName":"时间序列规整_1","preNodes":[{"checked":true,"id":"模拟时间序列_XFdCFEE5"}],"rerun":"false"},"inputTableName":"模拟时间序列_XFdCFEE5","timeSeriesFormat":{"reduction":"first","startEndTime":["2017-01-01 00:00:00","2019-01-01 00:00:00"],"timeColName":[{"datatype":"timestamp","index":1,"name":"dayTime"}],"timeWindowIdCol":"windowIdCol","value":"timeCol","windowLength":"2","windowUnit":"month"},"variablesColNames":[{"datatype":"int","index":2,"name":"x"},{"datatype":"int","index":3,"name":"y"},{"datatype":"int","index":4,"name":"z"}]}"""
    val parser = new JsonParser()
    val pJsonParser = parser.parse(jsonparam).getAsJsonObject
    val rddTableName = "时间序列规整_1_pKdufqmW"

    val tableName = pJsonParser.get("inputTableName").getAsString

    val rawDataFrame: DataFrame = try {
      outputrdd.get(tableName).asInstanceOf[DataFrame]
    } catch {
      case e: Exception => throw new Exception(s"获取数据表失败，具体信息${e.getMessage}")
    }

    val sQLContext = rawDataFrame.sqlContext
    val sparkContext = sQLContext.sparkContext

    // 数据只能为Numeric类型
    val variablesColObj = pJsonParser.get("variablesColNames").getAsJsonArray

    val variablesColNames = Array.range(0, variablesColObj.size()).map {
      i =>
        val name = variablesColObj.get(i).getAsJsonObject.get("name").getAsString
        Tools.columnTypesIn(name, rawDataFrame, true, StringType, DoubleType, IntegerType, LongType, FloatType, ByteType)
        name
    }

    val rawDataDF = rawDataFrame.na.drop(variablesColNames) // 只要有一个变量为空就会drop

    val timeSeriesFormatObj = pJsonParser.get("timeSeriesFormat").getAsJsonObject // "timeCol" // "timeIdCol"


    /** 1)获得时间序列窗口的id */
    val binningIdColName = timeSeriesFormatObj.get("timeWindowIdCol").getAsString
    Tools.validNewColName(binningIdColName, rawDataDF)

    val (dFAfterReduction: DataFrame, endWindowId: Long) = timeSeriesFormatObj.get("value").getAsString match {
      case "timeCol" =>

        /** 时间序列起止时间和窗宽信息 */
        val timeColName = timeSeriesFormatObj.get("timeColName")
          .getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString

        val (startTime: Long, endTime: Long) = try {
          val interval = timeSeriesFormatObj.get("startEndTime").getAsJsonArray
          val timeParser = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          (timeParser.parse(interval.get(0).getAsString).getTime,
            timeParser.parse(interval.get(1).getAsString).getTime)
        } catch {
          case e: Exception => throw new Exception(s"在获得时间序列起止时间过程中异常。具体信息${e.getMessage}")
        }

        val windowUnit: String = try {
          timeSeriesFormatObj.get("windowUnit").getAsString
        } catch {
          case e: Exception => throw new Exception(s"窗宽单位获取过程中失败，具体信息${e.getMessage}")
        }
        val windowLength: Int = try {
          timeSeriesFormatObj.get("windowLength").getAsString.toDouble.toInt
        } catch {
          case e: Exception => throw new Exception(s"您输入的时间序列窗宽长度不能转为数值类型, 具体信息为${e.getMessage}")
        }
        require(windowLength > 0, "您输入的时间序列窗宽长度需要大于0")

        /** 确认start和end恰好是整数个窗宽 */
        val binningEndTime = ToolsForTimeSeriesWarp.binning(
          new Timestamp(endTime), startTime, windowLength, windowUnit)

        require(binningEndTime.getTime == endTime, s"您输入的'时间序列起止时间'和'时间序列窗宽'参数不符合逻辑：" +
          s"算子要求您输入的起止时间应该恰好是您输入的窗宽的整数倍：" +
          s"您输入的起始时间是${new Timestamp(startTime)}, 窗宽是${windowLength + windowUnit}, " +
          s"整数倍窗宽的截止时间应为$binningEndTime, 请您按需求调节起始和终止时间.")

        val endWindowId = ToolsForTimeSeriesWarp.binningForId(
          new Timestamp(endTime), startTime, windowLength, windowUnit) - 1 // 左闭右开区间，所以结束时间减掉1才是最大窗口id


        /** 将窗宽分箱获得 => 时间 + 箱子所在id */

        val binningUDF = NullableFunctions.udf(
          (time: Timestamp) => ToolsForTimeSeriesWarp.binningForId(time, startTime, windowLength, windowUnit)
        )

        val df = rawDataDF.withColumn(binningIdColName, binningUDF(col(timeColName)))
          .filter(col(binningIdColName).between(0, endWindowId))

        /** 2)每个窗口进行规约 --规约方式可以为最大、最小、均值、最早四种方式，如果一个窗口内某个变量全为null值则规约为Double.NaN值 */
        val reduction = timeSeriesFormatObj.get("reduction").getAsString // "min", "mean", "first"
      val dFAfterReduction: DataFrame = reduction match {
        case "max" =>
          df.groupBy(col(binningIdColName))
            .agg(max(col(variablesColNames.head)).cast(DoubleType).alias(variablesColNames.head),
              variablesColNames.drop(1).map(name => max(col("`" + name + "`")).cast(DoubleType).alias(name)): _*)

        case "min" =>
          df.groupBy(col(binningIdColName))
            .agg(min(col(variablesColNames.head)).cast(DoubleType).alias(variablesColNames.head),
              variablesColNames.drop(1).map(name => min(col("`" + name + "`")).cast(DoubleType).alias(name)): _*)

        case "mean" =>
          df.groupBy(col(binningIdColName))
            .agg(mean(col(variablesColNames.head)).cast(DoubleType).alias(variablesColNames.head),
              variablesColNames.drop(1).map(name => mean(col("`" + name + "`")).cast(DoubleType).alias(name)): _*)

        case "first" =>
          df.sort(col(timeColName)).groupBy(col(binningIdColName))
            .agg(first(col(variablesColNames.head)).cast(DoubleType).alias(variablesColNames.head),
              variablesColNames.drop(1).map(name => first(col("`" + name + "`")).cast(DoubleType).alias(name)): _*)
        // 验证一下集群上是不是这样
      }

        (dFAfterReduction, endWindowId)

      case "timeIdCol" =>
        val schema = rawDataDF.schema.fields
        val endWindowId = try {
          rawDataDF.count() - 1
        } catch {
          case e: Exception => throw new Exception(s"算子在为无时间列的时间序列建立窗口时事变，可能的原因是数据为空。" +
            s"具体异常信息: ${e.getMessage}")
        }

        val df = sQLContext.createDataFrame(rawDataDF.rdd.zipWithIndex().map {
          case (row, windowId) =>
            Row.merge(Row(windowId), row)
        }, StructType(StructField(binningIdColName, LongType) +: schema))

        (df, endWindowId)
    }

    /** 3)窗口补全和记录补全 */
    // 补全方式 "mean", "zero", "linear"
    def fillLinear(starRow: Seq[Double], gaps: Seq[Double], step: Int): Seq[Double] =
      starRow.zip(gaps).map { case (d1, gap) => d1 + gap * step }

    def windowComplement(dFAfterReduction: DataFrame, startWindowId: Long, endWindowId: Long, binningIdColName: String, format: String): DataFrame = {
      /** 1)根据窗口id生成PairRDD并相机补全最大最小值 */
      var rdd: RDD[(Long, Seq[Double])] = dFAfterReduction.rdd.map {
        row =>
          (row.getAs[Long](binningIdColName), row.toSeq.drop(1).map(_.asInstanceOf[Double]))
      }

      val ((minWindowId, minWindowValue), (maxWindowId, maxWindowValue)) =
        try {
          format match {
            case "mean" =>
              val row = dFAfterReduction.select(variablesColNames.map(name => mean(col(name))): _*).head()
              ((rdd.keys.min(), row.toSeq.map(_.asInstanceOf[Double])),
                (rdd.keys.max(), row.toSeq.map(_.asInstanceOf[Double])))
            case "zero" =>
              val row = Row.fromSeq(variablesColNames.map(_ => 0.0))
              ((rdd.keys.min(), row.toSeq.map(_.asInstanceOf[Double])),
                (rdd.keys.max(), row.toSeq.map(_.asInstanceOf[Double])))
            case "linear" =>
              (rdd.min()(Ordering.by[(Long, Seq[Double]), Long](_._1)),
                rdd.max()(Ordering.by[(Long, Seq[Double]), Long](_._1)))
          }
        } catch {
          case e: Exception => throw new Exception("在时间规整阶段获取最大时间分箱id失败，可能的原因是：" +
            "1）数据有过多缺失值，在缺失值阶段数据为空，2）数据的时间列没有在时间序列起止时间内导致过滤后为空，" +
            s"3）其他原因。具体信息: ${e.getMessage}")
        }

      if (minWindowId != startWindowId) {
        rdd = rdd.union(sparkContext.parallelize(Seq((startWindowId, minWindowValue))))
      } // 如果开头缺失则找它最近时间的值补全

      if (maxWindowId != endWindowId) {
        rdd = rdd.union(sparkContext.parallelize(Seq((endWindowId, maxWindowValue))))
      } // 如果结尾缺失则找它最近时间的值补全

      /** 2)获得非空分区的数目 */
      /** 最后一个非空分区的id */
      val numParts = rdd.mapPartitions {
        iter =>
          if (iter.isEmpty) Iterator.empty else Iterator(1)
      }.count().toInt

      println(s"算子检测到原始分区数为${rdd.partitions.length}, 实际分区数为$numParts, 现已将空分区去除")

      /** 3)根据窗口id进行重分区 */
      class PartitionByWindowId(numParts: Int) extends Partitioner {
        override def numPartitions: Int = numParts

        override def getPartition(key: Any): Int =
          key match {
            case i: Long =>
              val maxNumEachPartition = scala.math.ceil(endWindowId / numParts.toDouble) // 最小为1.0
              (i / maxNumEachPartition).toInt // 必定大于等于0且小于numParts [[maxWindowId]]为最大值
            case _: Exception =>
              throw new SparkException("key的类型不是long")
          }
      }
      val rddPartitionByWinId: RDD[(Long, Seq[Double])] = rdd.coalesce(numParts).repartitionAndSortWithinPartitions(
        new PartitionByWindowId(numParts)
      ) // 确保分区后的顺序

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

      val rePartitionRdd: RDD[(Long, (Long, Seq[Double]))] = rddPartitionByWinId.mapPartitionsWithIndex {
        (partitionId, iter) =>
          val partitionNum = partitionId.toLong << 33
          var i = 0
          val result = mutable.ArrayBuilder.make[(Long, (Long, Seq[Double]))]()
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

      val maxPartitionIndex = rePartitionRdd.mapPartitionsWithIndex {
        case (index, iter) =>
          if (iter.isEmpty) Iterator.empty else Iterator(index)
      }.max()

      /** 按起止id补全 */
      val resultRdd = rePartitionRdd.mapPartitionsWithIndex(
        (index, iters) => {
          val result = mutable.ArrayBuilder.make[(Long, (Long, Seq[Double]))]()

          val iter = iters.toArray.sortBy(_._2._1).toIterator

          var lastWindowId = 0L
          var plus = 0L // 循环计数器
          var lastRow: Seq[Double] = null
          var i = 0
          var lastKey = 0L
          while (iter.hasNext) {
            val (key, (windowId, row)) = iter.next()

            if (i == 0) {
              result += Tuple2(key + plus, (windowId, row))
              lastWindowId = windowId
              lastKey = key
              lastRow = row
              plus += 1
            } else {
              val gap = row.zip(lastRow).map(tup => (tup._1 - tup._2) / (windowId - lastWindowId)) // 等差数列线性填充
              for (step <- 1 to (windowId - lastWindowId).toInt) {
                val fillValue = if (format == "linear") fillLinear(lastRow, gap, step) else minWindowValue
                result += Tuple2(lastKey + plus,
                  (lastWindowId + step, if (step == (windowId - lastWindowId)) row else fillValue))
                plus += 1
              }

              lastWindowId = windowId
              lastRow = row
            }

            i += 1
          }

          if (index == maxPartitionIndex)
            result.result().toIterator
          else
            result.result().dropRight(1).toIterator
        }
      )

      sQLContext.createDataFrame(resultRdd.values.map { case (windowId, values) => Row.fromSeq(windowId +: values) },
        StructType(StructField(binningIdColName, LongType) +: variablesColNames.map(name => StructField(name, DoubleType)))
      )
    }

    /** 4)结果输出 */
    val newDataFrame = windowComplement(dFAfterReduction, 0L, endWindowId, binningIdColName, "linear")

    newDataFrame.show()
    newDataFrame.registerTempTable(rddTableName)
    newDataFrame.sqlContext.cacheTable(rddTableName)
    outputrdd.put(rddTableName, newDataFrame)
  }

  def testVAR(): Unit = {
    testTimeSeriesWarping()

    println("This is a test file.")
    /** 0)获得数据和一些初始信息 */
    val jsonparam = """{"RERUNNING":{"nodeName":"时间序列规整_1","preNodes":[{"checked":true,"id":"模拟时间序列_XFdCFEE5"}],"rerun":"false"},"inputTableName":"模拟时间序列_XFdCFEE5","timeSeriesFormat":{"reduction":"first","startEndTime":["2017-01-01 00:00:00","2019-01-01 00:00:00"],"timeColName":[{"datatype":"timestamp","index":1,"name":"dayTime"}],"timeWindowIdCol":"windowIdCol","value":"timeCol","windowLength":"2","windowUnit":"month"},"variablesColNames":[{"datatype":"int","index":2,"name":"x"},{"datatype":"int","index":3,"name":"y"},{"datatype":"int","index":4,"name":"z"}]}"""
    val parser = new JsonParser()
    val pJsonParser = parser.parse(jsonparam).getAsJsonObject
    val rddTableName = "时间序列规整_1_pKdufqmW"

    val tableName = pJsonParser.get("inputTableName").getAsString

    val rawDataFrame: DataFrame = try {
      outputrdd.get(tableName).asInstanceOf[DataFrame]
    } catch {
      case e: Exception => throw new Exception(s"获取数据表失败，具体信息${e.getMessage}")
    }

    val sQLContext = rawDataFrame.sqlContext
    val sparkContext = sQLContext.sparkContext

    rawDataFrame.show()


    val windowIdColName = "windowIdCol"
    Tools.columnTypesIn(windowIdColName, rawDataFrame, true, LongType)

    val variablesColObj = pJsonParser.get("variablesColNames").getAsJsonArray
    val variablesColNames = Array.range(0, variablesColObj.size()).map {
      i =>
        val name = variablesColObj.get(i).getAsJsonObject.get("name").getAsString
        Tools.columnTypesIn(name, rawDataFrame, true, StringType, DoubleType, IntegerType, LongType, FloatType, ByteType)
        name
    }


    /** VAR模型的信息 */
    val k = variablesColNames.length // k维
    val p = 3 // 阶
    val timeT = rawDataFrame.count()

    require(timeT < (1.toLong << 32), "目前算法要求时间序列数目不大于2的32次方")
    val requireCount = k * k * p + (k + 1) * k / 2
    require(timeT > requireCount.toLong, s"根据您输入的滞后阶数和变量列数，模型总共需要估计${requireCount}个参数，" +
      s"数据条数需要超过该数才满足过度识别条件") // k*k*p + (k + 1)*k/2

  }


  override def run(): Unit = {


    val data = Seq.range(0, 200).map(i => (i.toLong, Array(1, 2, 3.0)))
    val rdd = sc.makeRDD(data)

    val timeT = rdd.count()
    val p = 5


    def findMiddleTwo(width: Long, length: Long)(z: Long): Seq[Long] = {
      Seq(0L, z, z - width, length).sorted.slice(1, 3) // 取中间两个
    }

    case class Meta(lag: Int, value: Array[Double])

    val changeRdd: RDD[(Long, (Int, Array[Double]))] = rdd.flatMap {
      row =>
        val windowId = row._1
        val Seq(start, end) = findMiddleTwo(p, timeT - 1 - p )(windowId)

        val res = mutable.ArrayBuilder.make[(Long, (Int, Array[Double]))]()
        var dst_window = end
        while(dst_window >= start) {
          res += Tuple2(dst_window, Tuple2((windowId - dst_window).toInt, row._2))
          dst_window -= 1
        }
        res.result()
    }

    val zeroValue: mutable.Set[(Int, Array[Double])] = scala.collection.mutable.Set.empty
    val seqOp = (map: mutable.Set[(Int, Array[Double])], value: (Int, Array[Double])) => {
      map += value
      map
    }

    val resultRdd = changeRdd.aggregateByKey(zeroValue)(seqOp, (m1, m2) => m1 ++ m2).mapValues {
      set =>
        set.toArray.sortBy(_._1)
    }

    println("-"*80)

    resultRdd.collect().sortBy(_._1).foreach{
      case (key, arr) =>
        println(s"key:$key", s"lag: ${arr.map(_._1).mkString(",")}")
    }












  }
}
