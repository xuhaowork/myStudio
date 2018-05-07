package com.self.core.timeBinning.tests

import java.sql.Date
import java.text.SimpleDateFormat
import com.zzjz.deepinsight.basic.BaseMain
import com.zzjz.deepinsight.core.timeBinning.models.{NaturalTwoStepInfo, TimeBinning}
import com.zzjz.deepinsight.core.utils.TimeColInfo
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, from_unixtime, unix_timestamp}
import org.apache.spark.sql.types._
import org.joda.time.DateTime

/**
  * editor：Xuhao
  * date： 2017/1/4 10:00:00
  */

/**
  * 测试分箱
  */
object testSequenceBinning extends BaseMain {
  override def run(): Unit = {
//    /** 模拟数据源 */
//    val timeFormatTmp: String = "yyyy/MM/dd HH:mm:ss"
//    val lst = List(
//      Array("2017/02/10 00:00:00", 1, 2),
//      Array("2017/02/08 01:00:01", 1, 2),
//      Array("2017/3/1 04:00:02", 1, 2),
//      Array("2017/4/10 00:15:03", 1, 2),
//      Array(null, 1, 2),
//      Array("2017/04/20 07:20:05", 1, 2),
//      Array("2017/04/30 08:01:06", 1, 2),
//      Array("2017/04/30 09:11:06", 1, 2),
//      Array("2017/04/30 16:01:06", 1, 2),
//      Array("2017/06/10 13:01:06", 1, 2),
//      Array("2017/08/10 00:00:00", 1, 2),
//      Array("2017/08/18 01:00:01", 1, 2),
//      Array("2017/11/1 04:00:02", 1, 2),
//      Array("2017/12/31 00:15:03", 1, 2),
//      Array("2017/04/10 06:20:04", 1, 2),
//      Array("2018/01/1 07:20:05", 1, 2),
//      Array("2018/02/19 13:01:06", 1, 2),
//      Array("2018/03/2 13:01:06", 1, 2),
//      Array("2018/03/9 13:01:06", 1, 2),
//      Array("2018/04/1 13:01:06", 1, 2))
//
//
//    val df_rdd = sc.parallelize(lst)
//      .map(Row.fromSeq(_))
//
//    val df = sqlc.createDataFrame(df_rdd, StructType(
//      Array(StructField("time", StringType), StructField("id", IntegerType), StructField("age", IntegerType))))
//
//    val s = df.withColumn("utc", unix_timestamp(col("time"), timeFormatTmp)) //精确到毫秒
//    val rawDataDF = s.withColumn("stampTime", col("utc").*(1000).cast(TimestampType))
//
//    rawDataDF.show(200)
////
////    val timeColInfo = new TimeColInfo("time", "string", Some("yyyy/MM/dd HH:mm:ss"))
////    val binningInfo = new NaturalTwoStepInfo("month", "hour")
////    val binner = new TimeBinning(timeColInfo, binningInfo)
////    binner.transform(rawDataDF).show()
////
////    binner.binning(rawDataDF).show()
//
//
//
//
//
//
        println("good")

        val timeFormat = new SimpleDateFormat("HH时mm分ss秒")
        val timeStamp = timeFormat.parse("14时23分08秒").getTime
        val dateTime = new DateTime(timeStamp)

    println(dateTime)
        val floorTime = dateTime.monthOfYear().roundFloorCopy()
        println(floorTime.toString("yyyy-MM-dd HH:mm:ss"))
    println(new DateTime(dateTime.getMillis - floorTime.getMillis).hourOfDay().roundHalfFloorCopy().toString("yyyy-MM-dd HH:mm:ss"))

    println(new DateTime(dateTime.getMillis - floorTime.getMillis).dayOfYear().roundHalfFloorCopy().toString("yyyy-MM-dd HH:mm:ss"))




    val dt = new DateTime()
    println(dt.toString("yyyy-MM-ddEEE")) //dt.toString("EEE", Locale.FRENCH)




  }


}
