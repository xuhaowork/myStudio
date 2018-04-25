package com.self.core.VAR

import com.self.core.baseApp.myAPP
import org.joda.time.DateTime
import java.text.SimpleDateFormat

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

object TestVAR extends myAPP{
  override def run(): Unit = {
    /**
      * 1. 数据初始提取
      */
    // 1.id列
    val hasIdCol = "true" // 是否有id列
    val (idColName, idColType) = ("id", "string")

    // 2.时间序列信息
    val timeColFormat = "true"
    /** 分为三种：
      * 1)无时间列 => 以数据顺序为时间序列; false
      * 2)有时间列; true
      *     A.输入时间列名                       || timeCol
      *     B.输入字段类型                       || fieldFormat -- StringType, UTC, TimeStampType
      *     B.输入时间列格式                     || timeFormat
      *     C.输入时间序列起止时间                || startTime, endTime
      *     D.请选择时间序列频率划分方式           || frequencyFormat
      *         i)以固定长度时间划分                    || fixedLength
      *         输入长度                                   || frequencyLength
      *         输入长度单位                                || unit
      *         ii)以自然时间划分                       || naturalLength
      *         输入时间单位                                || unit
      *           if(星期) 工作日 or 全周                         -- weekday, all
      */
    val timeCol = "dayTime"
    val fieldFormat = "StringType"
    val timeFormat = "yyyy-MM-dd HH:mm:ss"

    val startTime = "2017-12-10 00:00:00"
    val endTime = "2017-12-10 00:10:00"

    val timeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val startTimeStamp = timeParser.parse(startTime).getTime
    val endTimeStamp = timeParser.parse(endTime).getTime

    val frequencyFormat = "fixedLength" // fixedLength, naturalLength
    val frequencyLength = "10"
    val unit = "week"

    val frequency: Int = { util.Try(frequencyLength.toInt) getOrElse 1 } * (unit match {
      case "week" => 604800000
      case "day" => 86400000
      case "hour" => 3600000
      case "minute" => 60000
      case "second" => 1000
      case "microsecond" => 1
    })

    // 特征列
    val featureCols: ArrayBuffer[(String, String)] = ArrayBuffer(("x", "int"), ("y", "int"), ("z", "int"))

    /**
      * 2. 模拟数据
      */
    val arr = List(
      Array("1", "2017-12-10 00:00:00", 123, -150, -150),
      Array("1", "2017-12-10 00:01:00", 9, -150, -150),
      Array("1", "2017-12-10 00:03:00", 19, -10, -150),
      Array("1", "2017-12-10 00:01:01", 19, -150, -150),
      Array("1", "2017-12-10 00:02:00", 199, 50, -150),
      Array("1", "2017-12-10 00:04:00", 199, -150, -150),
      Array("1", "2017-12-10 00:09:00", 199, 0, 0),
      Array("2", "2017-12-10 00:07:00", 99, 0, -150),
      Array("2", null, 199, -150, -150),
      Array("2", "2017-12-10 00:01:00", 1, -50, -15),
      Array("2", "2017-12-10 00:02:02", 90, -0, -1),
      Array("2", "2017-12-10 00:02:02", 19, -50, -15))

    val rdd = sc.parallelize(arr).map(Row.fromSeq(_))
    var rawDataDF = sqlc.createDataFrame(rdd, StructType(
      Array(StructField("id", StringType),
        StructField("dayTime", StringType),
        StructField("x", IntegerType),
        StructField("y", IntegerType),
        StructField("z", IntegerType))))



    val col_type = rawDataDF.schema.fields.map(_.dataType)
      .apply(rawDataDF.schema.fieldIndex(timeCol))

    val newTimeCol = timeCol + "_new"
    rawDataDF = fieldFormat match {
      case "StringType" =>
        if(col_type != StringType){
          throw new Exception(s"$timeCol 不是 StringType")
        }else{
          rawDataDF.withColumn(newTimeCol, unix_timestamp(col(timeCol), timeFormat).*(1000))
        }
      case "TimeStampType" =>
        if(col_type != TimestampType)
          throw new Exception(s"$timeCol 不是 TimeStampType")
        rawDataDF.withColumn(newTimeCol, col(timeCol).cast(LongType).*(1000))
      case "UTC" => try {
        rawDataDF.withColumn(newTimeCol, col(timeCol).cast(LongType).*(1000))
      } catch {
        case _: Exception =>
          throw new Exception(s"$timeCol 不是 LongType")
      }
    }

    rawDataDF.show()

    if(hasIdCol == "true"){
      val rawRdd = rawDataDF.rdd.map(r => {
        val id: String = idColType match {
          case "int" => util.Try(r.getAs[Int](idColName).toString) getOrElse null
          case "float" => util.Try(r.getAs[Float](idColName).toString) getOrElse null
          case "double" => util.Try(r.getAs[Double](idColName).toString) getOrElse null
          case "string" => util.Try(r.getAs[String](idColName).toString) getOrElse null
        }

        val arr = featureCols.map{
          case (name, colType) => colType match {
            case "int" => util.Try(r.getAs[Int](name).toDouble) getOrElse 0.0
            case "float" => util.Try(r.getAs[Float](name).toDouble) getOrElse 0.0
            case "double" => util.Try(r.getAs[Double](name)) getOrElse 0.0
            case "string" => util.Try(r.getAs[String](name).toDouble) getOrElse 0.0
          }
        }

        val time = util.Try(r.getAs[Long](newTimeCol)) getOrElse 0L // 时间

        /** 时间转换 => 按给定的时间序列频率规则给出一个分界时间 & 还有和分界时间的时间差(当冲突时取最近时间的记录) */
        val dt = new DateTime(time)
        val roundTime = dt.yearOfCentury().roundFloorCopy().getMillis
        val modTime = time - roundTime
        ((id, roundTime), (modTime, arr))
      })


    }



















  }
}
