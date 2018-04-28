//package com.zzjz.deepinsight.core.TimeSeries.VAR
//
//import com.google.gson.{Gson, JsonParser}
//import com.zzjz.deepinsight.basic.BaseMain
//import com.zzjz.deepinsight.core.TimeSeries.utils.TransformUtils
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.DoubleType
//import scala.collection.mutable.ArrayBuffer
//import org.apache.spark.mllib.linalg.DenseMatrix
//import org.apache.spark.mllib.timeSeries.VAR
//import com.cloudera.sparkts.{DateTimeIndex, DayFrequency, TimeSeries}
//
//
///**
//  * editor: xuhao
//  * date: 2018-04-20 10:30:00
//  */
//
///**
//  * VAR本地算子
//  * ----
//  * 时间序列中的VAR模型
//  * ----
//  * 此处为本地执行，只能执行同一个id的时间序列
//  *
//  */
//object VARLocal extends BaseMain{
//  override def run(): Unit = {
//    /**
//      * 一些参数的处理
//      */
//    /** 0)获取基本的系统变量 */
//    val jsonparam = "<#zzjzParam#>"
//    val gson = new Gson()
//    val p: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])
//    val z1 = z
//    val rddTableName = "<#zzjzRddName#>"
//
//    /** 1)获取DataFrame */
//    val tableName = p.get("inputTableName").trim
//    val rawDataDF = z1.rdd(tableName).asInstanceOf[org.apache.spark.sql.DataFrame]
//    val parser = new JsonParser()
//    val pJsonParser = parser.parse(jsonparam).getAsJsonObject
//
//    /** 2)获取对应的变量 */
//    val bufferCols = pJsonParser.getAsJsonArray("bufferCols")
//    var buffer = ArrayBuffer.empty[String]
//    for(i <- 0 until bufferCols.size()){
//      val colName = bufferCols.get(i).getAsJsonObject.get("name").getAsString
//      buffer += colName
//    }
//
//    /** 3)滞后阶数和预测步数 */
//    val P = p.get("P").toInt
//    val pSteps = p.get("pSteps").toInt
//
//    val inputDf = rawDataDF.select(buffer.map(name => col(name).cast(DoubleType)):_*)
//
//    val tsArray = TransformUtils.DataFrameToColArray(inputDf)
//    val rowLength = tsArray.length
//    val colLength = tsArray.head.length
//    val bdm = new DenseMatrix(rowLength, colLength, tsArray.flatten, true)
//
//
//    val timeIndex = DateTimeIndex.uniform(0L, rowLength, new DayFrequency(1))
//    val ts = new TimeSeries[String](timeIndex, bdm, Array.tabulate(colLength)(i => "type_" + i))
//
//    val varModel = new VAR(P, pSteps).run(ts)
//
//    /** 输出结果 */
//    outputrdd.put("结果", rddTableName + "_" + varModel)
//
//
//
//  }
//}
