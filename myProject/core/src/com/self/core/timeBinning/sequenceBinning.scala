package com.self.core.timeBinning

import com.zzjz.deepinsight.basic.BaseMain
import java.sql.Date
import java.text.SimpleDateFormat

import scala.util.Try
import com.google.gson.{Gson, JsonParser}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

/**
  * editor：Xuhao
  * date： 2017/10/04 10:00:00
  */

/**
  * 顺序分箱（最老的版本，现已被sequenceTimeBinning中的sequenceBinnerObj取代）
  */
object sequenceBinning extends BaseMain{
  override def run(): Unit = {
    val jsonparam = "<#zzjzParam#>"
    val gson = new Gson()
    val p: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])

    val z1 = z

    val rddTableName = "<#zzjzRddName#>"
    val tableName = p.get("inputTableName").trim
    val rawDataDF = z1.rdd(tableName).asInstanceOf[org.apache.spark.sql.DataFrame]
    val colnames: String = p.get("colnames").trim
    val bucket = p.get("bucket").trim
    val timeFormat = p.get("timeFormat")
    val stepLength = p.get("stepLength").toDouble
    val stepUnit = p.get("stepUnit")


    val parser = new JsonParser()
    val pJsonParser = parser.parse(jsonparam).getAsJsonObject
    val timeAxisObj = pJsonParser.getAsJsonObject("timeAxis")
    println(timeAxisObj)
    val timeAxis = timeAxisObj.get("value").getAsString

    println(timeAxis)

    val (start_time, end_time) = timeAxis match {
      case "byHand" =>
        val timeString = timeAxisObj.get("byHandInterval").getAsString.split(",")
        (timeString(0).trim, timeString(1).trim)
      case "minMax" => throw new Exception("Sorry, 暂不支持该功能，敬请期待")
      case "smoothMinMax" => throw new Exception("Sorry, 暂不支持该功能，敬请期待")
    }


    val presentationFormat = p.get("presentationFormat").toString.trim


    def getUnit(unit: String): Int = {
      unit match {
        case "d" => 24 * 3600 * 1000
        case "h" => 3600 * 1000
        case "m" => 60 * 1000
        case "s" => 1000
        case _ => throw new Exception("周期单位只能为：天、小时、分、秒，对应符号为：d、h、m、s")
      }
    }

    val timeFmt: SimpleDateFormat = timeFormat match {
      case "TimestampType" => new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      case "UTC" => new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      case _ => new SimpleDateFormat(timeFormat)
    }


    val formatDate = (date: Long, timeFmt:  SimpleDateFormat) => {
      val theDate = new Date(date)
      timeFmt.format(theDate)
    }

    val broadFormatDate = sc.broadcast(formatDate)


    val start_timeStamp = timeFmt.parse(start_time).getTime
    val end_timeStamp = timeFmt.parse(end_time).getTime

    val real_timeFormat = timeFormat match {
      case "TimestampType" => "yyyy/MM/dd HH:mm:ss"
      case "UTC" => "yyyy/MM/dd HH:mm:ss"
      case _ => timeFormat
    }



    // val new_df = {
    // 根据起止时间和步长构建splits
    val splits = start_timeStamp.to(end = end_timeStamp + stepLength.toLong * getUnit(stepUnit), step = stepLength.toLong * getUnit(stepUnit))
      .toList.map(_.toDouble).toArray

    val splitsBC: Broadcast[Array[Double]] = sc.broadcast(splits)

    // 将数据转换为带时间戳的rdd
    val schemaFields = rawDataDF.schema.fields

    var df_with_timeStamp = timeFormat match {
      case "TimestampType" => {
        rawDataDF.withColumn("timeStamp", col(colnames).cast(LongType)*1000)
      }
      case "UTC" => {
        rawDataDF.withColumn("timeStamp", col(colnames).cast(LongType)*1000)
      }
      case _ => {
        val new_rdd = rawDataDF.rdd.map(x => {
          var array = x.toSeq.toBuffer
          val dreBegin_time = x(x.fieldIndex(colnames)).toString
          val timeStamp: Long = scala.util.Try(timeFmt.parse(dreBegin_time).getTime) getOrElse -1L
          array += timeStamp
          Row.fromSeq(array)
        })
        var v = schemaFields.toBuffer
        v += StructField("timeStamp", LongType)
        val newSchemaName = v.toArray
        sqlc.createDataFrame(new_rdd, StructType(newSchemaName))
      }
    }


    df_with_timeStamp = df_with_timeStamp.where("timeStamp >= " + start_timeStamp + "  and timeStamp <= " + end_timeStamp)
    df_with_timeStamp = df_with_timeStamp.withColumn("timeStamp", col("timeStamp").cast(DoubleType))

    df_with_timeStamp.show()


    val bucketizer = new Bucketizer()
      .setInputCol("timeStamp")
      .setOutputCol(bucket)
      .setSplits(splits)
    var bucketedData = bucketizer.transform(df_with_timeStamp)
    // val new_df = bucketedData


    //   bucketedData = presentationFormat match {
    //     case "numericFormatWithNotID" => bucketedData
    //     case "timeFormatWithID" => {
    //       // 以时间显示
    //       val time_format_rdd = bucketedData.rdd.map(x => {
    //         val binningID = x(x.fieldIndex(bucket)).toString.toDouble
    //         val tmp = splits(binningID.toInt)/1000
    //         // val binningTime = formatDate(tmp.toLong, timeFmt)
    //         var array = x.toSeq.toBuffer
    //         // array += binningTime
    //         array += tmp.toLong
    //         Row.fromSeq(array)
    //       })

    //       var tmp = sqlContext.createDataFrame(time_format_rdd, {
    //         val newSchemaFields = bucketedData.schema.fields
    //         var w = newSchemaFields.toBuffer
    //         w += StructField(bucket + "_timeFormat", LongType)
    //         StructType(w.toArray)
    //       })
    //       tmp = tmp.drop("timeStamp")
    //       tmp = tmp.withColumn(bucket + "_timeFormat", from_unixtime(col(bucket + "_timeFormat"), real_timeFormat))
    //       tmp.withColumn("bucket", col("bucket").cast(IntegerType))
    //     }
    //   }

    bucketedData = presentationFormat match {
      case "numericFormatWithNotID" => bucketedData
      case "timeFormatWithID" => {
        // 以时间显示
        val time_format_rdd = bucketedData.rdd.map(x => {
          val binningID = x(x.fieldIndex(bucket)).toString.toDouble
          val tmp = splitsBC.value(binningID.toInt)
          val tmpEnd = splitsBC.value(binningID.toInt + 1)
          //            val binningTime = formatDate(tmp.toLong, timeFmt)
          var array = x.toSeq.toBuffer


          val s = broadFormatDate.value(tmp.toLong,timeFmt)
          val e = broadFormatDate.value(tmpEnd.toLong,timeFmt)
          array += s
          array += (s + "," + e)
          Row.fromSeq(array)
        })

        var tmp = sqlc.createDataFrame(time_format_rdd, {
          val newSchemaFields = bucketedData.schema.fields
          var w = newSchemaFields.toBuffer
          w += StructField(bucket + "_timeFormat", StringType)
          w += StructField(bucket + "_timeInterval", StringType)
          StructType(w.toArray)
        })
        tmp = tmp.drop("timeStamp")
        //          tmp = tmp.withColumn(bucket + "_timeFormat", from_unixtime(col(bucket + "_timeFormat"), real_timeFormat))
        //          tmp = tmp.withColumn(bucket + "_timeFormatEnd", from_unixtime(col(bucket + "_timeFormatEnd"), real_timeFormat))
        tmp.withColumn("bucket", col("bucket").cast(IntegerType))
      }
    }

    val new_df = bucketedData
    // }


    new_df.show()
    new_df.cache()
    outputrdd.put("<#zzjzRddName#>", new_df)
    new_df.registerTempTable("<#zzjzRddName#>")
    sqlc.cacheTable("<#zzjzRddName#>")

    /**
      * 新增部分
      */
    val intervalArray = splits.map(e => formatDate(e.toLong, timeFmt))
    val outDf = sqlc.createDataFrame(sc.parallelize(intervalArray.map(e => Row(e))),StructType(Array(StructField("interval",StringType,false))))
    outputrdd.put("分箱间隔存储", outDf)
    outDf.registerTempTable("分箱间隔存储")
    sqlc.cacheTable("分箱间隔存储")





  }



}
