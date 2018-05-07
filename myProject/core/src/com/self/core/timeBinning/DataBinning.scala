package com.self.core.timeBinning

import java.sql.Date
import java.text.SimpleDateFormat

import com.google.gson.{Gson, JsonParser}
import com.zzjz.deepinsight.basic.BaseMain
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.col



/**
  * editor：Xuhao
  * date： 2017/10/04 10:00:00
  */

/**
  * 分箱：周期+顺序
  */

object DataBinning extends BaseMain{
    override def run(): Unit = {
      /**
        * 参数设定
        */
      // --------
      // 表名  inputTableName
      // 分箱的列名 colnames
      // 时间格式 timeFormat {yyyy-MM-dd,yyyy/MM/dd,yyyy-MM-dd HH:mm:ss,yyyy/MM/dd HH:mm:ss,yyyy-MM-dd HH:mm:ss:SSS
      //                      yyyy/MM/dd HH:mm:ss:SSS,UTC}
      // 步长 stepLength
      // 步长单位（单位为天、小时、分钟、秒，分别为d、h、m、s） stepUnit {d,h,m,s}
      // 时间轴起始值：手动设定起始值|自动最大最小时间作为起始值|自动最大最小并平滑近似到小时、天、月  timeAxis（最终会返回时间起始值）
      //                   --if 手动设定起始值 byHand
      //                        --时间轴上下界（英文逗号分隔，格式与时间格式相同） byHandInterval
      //                   --if 自动以最大最小时间作为时间轴区间 minMax
      //                   --if 最大最小时间平滑近似后作为时间轴区间 smoothMinMax
      //                        --平滑近似单位  smoothUnit {y,M,d,h,m,s}

      // 分箱方式： binningFormat
      //                   --if 顺序分箱 sequenceBinning
      //                   --if 周期分箱
      //                        --周期  period
      //                        --周期单位（单位为天、小时、分钟、秒，分别为d、h、m、s） periodUnit {d,h,m,s}
      //                        --周期相位（填入任意一个周期的开始时间，要求与时间格式一致，默认为最小时间平滑精确到天）phase
      //                        --周期内分箱区间起始值（起始值要求是0到周期之内的数值，英文逗号分隔） cycleInterval
      //                        --周期分箱区间单位  intervalUnit {d,h,m,s}
      //                        --是否考虑周期顺序  includeCycleId {yes,no}
      // 分箱结果显示方式：presentationFormat {binningID,timeFormat}


      /**
        * 参数
        */
      val jsonparam = "<#jsonparam#>"
      val gson = new Gson()
      val p : java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String,String]])

      // The input parameter:
      // ------------
      val rddTableName = "<#rddtablename#>"
      val tableName = p.get("inputTableName").trim
      val rawDataDF = z.rdd(tableName).asInstanceOf[org.apache.spark.sql.DataFrame]
      val colnames: String = p.get("colnames").trim
      val bucket = p.get("bucket").trim
      val timeFormat = p.get("timeFormat")
      val stepLength = p.get("stepLength").toDouble
      val stepUnit = p.get("stepUnit")


      // 解析嵌套的json
      val parser = new JsonParser()
      val pJsonParser = parser.parse(jsonparam).getAsJsonObject
      val timeAxisObj = pJsonParser.getAsJsonObject("timeAxis")
      val timeAxis = timeAxisObj.get("timeAxis").getAsString

//      var start_time = ""
//      var end_time = ""
      val (start_time, end_time) = timeAxis match {
        case "byHand" =>
          val timeString = timeAxisObj.get("byHandInterval").getAsString.split(",")
          (timeString(0).trim, timeString(1).trim)

        case "minMax" => throw new Exception("Sorry, 暂不支持该功能，敬请期待")
        case "smoothMinMax" => throw new Exception("Sorry, 暂不支持该功能，敬请期待")
      }



      val binningFormatObj = pJsonParser.getAsJsonObject("binningFormat")
      val binningFormat = binningFormatObj.get("binningFormat").getAsString






//      /**
//        * 测试数据
//        */
//      val path = "G://网络行为分析数据/ipsession.csv"
//      val header = "true"
//      var rawDataDF = sqlc.read.format("com.databricks.spark.csv")
//        .option("header", header).option("inferSchema", true.toString).load(path)
//      rawDataDF = rawDataDF.withColumnRenamed(rawDataDF.columns(0), "DRETBEGINTIME")
//
//
//
//       表名 inputTableName
//
//       分箱的列名
//      val colnames = "DRETBEGINTIME"
//
//       输出的分箱列名
//      val bucket = "bucket"
//
//       假定选择时间数据分箱
//      val timeFormat: String = "yyyy/MM/dd HH:mm:ss"
//
//       步长
//      val stepLength = 3
//
//       步长单位
//      val stepUnit = "h"
//
//      // 时间轴起始值(从timeAxis得到)
//      val start_time = "2017/1/18 00:00:00"
//      val end_time = "2017/1/22 00:00:00"
//
//       分箱方式
//      val binningFormat = "sequenceBinning"
//      val binningFormat = "cycleBinning"
//
//
//
//
//
//      // 周期相位
//      val phase = "2017/1/18 00:00:00"
//
//      // 周期分箱区间单位
//      val intervalUnit = "h"
//      // 周期内分箱区间
//      val interval_start = 9
//      val interval_end = 15
//
////      // 是否考虑周期顺序
////      val includeCycleId = "no"
//
//      // 分箱周期设定一个初始值
//      var period = 3.0
//
//      // 周期选择
//      val periodSelect = "unitPeriod"
//
//      // 周期的覆盖
//      if(periodSelect == "userDefinedPeriod")
//        period = 2.0
//
//      // 周期单位
//      val periodUnit = "d"
//
//
//      // 输出的格式：时间格式或者ID编号
//      val presentationFormat = "timeFormatWithID"


      /**
        * 一些用到的工具设定（公共）
        */
      // 将单位由字符转为数值
      def getUnit(unit: String): Int = {
        unit match {
          case "d" => 24 * 3600 * 1000
          case "h" => 3600 * 1000
          case "m" => 60 * 1000
          case "s" => 1000
          case _ => throw new Exception("周期单位只能为：天、小时、分、秒，对应符号为：d、h、m、s")
        }
      }
      val timeFmt: SimpleDateFormat = new SimpleDateFormat(timeFormat)


      // 时间戳转时间格式
      def formatDate(date: Long, timeFormat: String) = {
        val timeFmt: SimpleDateFormat = new SimpleDateFormat(timeFormat)
        val theDate = new Date(date)
        timeFmt.format(theDate)
      }


      val start_timeStamp = timeFmt.parse(start_time).getTime
      val end_timeStamp = timeFmt.parse(end_time).getTime



      val new_df = binningFormat match {
          // case1
        case "sequenceBinning"  => {
          // 根据起止时间和步长构建splits
          val splits = start_timeStamp.to(end = end_timeStamp, step = stepLength.toLong*getUnit(stepUnit)).toList.map(_.toDouble).toArray
          val splitsBC: Broadcast[Array[Double]] = sc.broadcast(splits)

          // 将数据转换为带时间戳的rdd
          val schemaFields = rawDataDF.schema.fields
          val new_rdd = rawDataDF.rdd.map(x => {
            var array = x.toSeq.toBuffer
            val dreBegin_time = x(x.fieldIndex(colnames)).toString
            val timeStamp: Double = util.Try(timeFmt.parse(dreBegin_time).getTime.toDouble) getOrElse -1.0
            array += timeStamp.toDouble
            Row.fromSeq(array)
          })

          var v = schemaFields.toBuffer
          v += StructField("timeStamp", DoubleType)
          val newSchemaName = v.toArray

          // 将新的rdd变为dataframe
          var df_with_timeStamp = sqlc.createDataFrame(new_rdd, StructType(newSchemaName))

          // 筛选出给定的时间，如果筛选时间不为空的话，否则不用筛选。这样会自动忽略无法解析的数据
          df_with_timeStamp = df_with_timeStamp.where(s"timeStamp >= $start_timeStamp and timeStamp <= $end_timeStamp")


          // 分箱
          val bucketizer = new Bucketizer()
            .setInputCol("timeStamp")
            .setOutputCol(bucket)
            .setSplits(splitsBC.value)
          var bucketedData = bucketizer.transform(df_with_timeStamp)

          val presentation = binningFormatObj.get("presentationFormat").getAsString

          if(presentation == "timeFormat"){
            // 以时间显示
            val time_format_rdd = bucketedData.rdd.map(x => {
              val binningID = x(x.fieldIndex(bucket)).toString.toDouble
              val tmp = splitsBC.value(binningID.toInt)
              val binningTime = formatDate(tmp.toLong, timeFormat)
              var array = x.toSeq.toBuffer
              array += binningTime
              Row.fromSeq(array)
            })

            bucketedData = sqlc.createDataFrame(time_format_rdd, {
              val newSchemaFields = bucketedData.schema.fields
              var w = newSchemaFields.toBuffer
              w += StructField(bucket + "_timeFormat", StringType)
              StructType(w.toArray)})
            bucketedData = bucketedData.drop("timeStamp")
          }
          bucketedData
        }

          // case2
        case "cycleBinning" => {
          val periodSelectObj = binningFormatObj.getAsJsonObject("periodSelect")
          val periodSelect = periodSelectObj.get("periodSelect").getAsString
          var period  = 1.0
          var periodUnit = "h"
          var presentationFormat = "timeFormatWithID"

          if(periodSelect == "unitPeriod"){
            periodUnit = periodSelectObj.get("periodUnit").getAsString
            presentationFormat = periodSelectObj.get("presentationFormat").getAsString

          }else{
            periodUnit = periodSelectObj.get("periodUnit").getAsString
            period = periodSelectObj.get("period").getAsString.toDouble
            presentationFormat = periodSelectObj.get("presentationFormat").getAsString
          }

          val cycleInterval = periodSelectObj.get("cycleInterval").getAsString
          val (interval_start, interval_end) = {
            val u = cycleInterval.split(",")
            (u(0).trim.toDouble, u(1).trim.toDouble)
          }

          // 根据起止时间和步长构建splits
          val splits = (interval_start*getUnit(stepUnit)).
            to(end = interval_end*getUnit(stepUnit), step = stepLength*getUnit(stepUnit)).
            toList.map(_.toDouble).toArray
          val splitsBC: Broadcast[Array[Double]] = sc.broadcast(splits)


          // 将数据转换为带时间戳的rdd
          val schemaFields = rawDataDF.schema.fields
          val new_rdd = rawDataDF.rdd.map(x => {
            var array = x.toSeq.toBuffer
            val dreBegin_time = x(x.fieldIndex(colnames)).toString
            val timeStamp: Double = util.Try(timeFmt.parse(dreBegin_time).getTime.toDouble) getOrElse -1.0
            val periodTimeStamp: Long = timeStamp.toLong % (period * getUnit(periodUnit)).toLong
            val phases: Double = (timeStamp.toLong / (period * getUnit(periodUnit)).toLong) * (period * getUnit(periodUnit))
            array += timeStamp.toDouble
            array += periodTimeStamp.toDouble
            array += phases.toDouble
            Row.fromSeq(array)
          })

          var v = schemaFields.toBuffer
          v += StructField("timeStamp", DoubleType)
          v += StructField("periodTimeStamp", DoubleType)
          v += StructField("phasesTimeStamp", DoubleType)
          val newSchemaName = v.toArray

          // 将新的rdd变为dataframe
          var df_with_timeStamp = sqlc.createDataFrame(new_rdd, StructType(newSchemaName))

          // 筛选出给定的时间，如果筛选时间不为空的话，否则不用筛选。这样会自动忽略无法解析的数据
          val start_timeStamp = timeFmt.parse(start_time).getTime
          val end_timeStamp = timeFmt.parse(end_time).getTime
          df_with_timeStamp = df_with_timeStamp.where(s"periodTimeStamp >= ${interval_start*getUnit(stepUnit)}" +
            s" and periodTimeStamp <= ${interval_end*getUnit(stepUnit)}" +
            s" and timeStamp >= $start_timeStamp and timeStamp <= $end_timeStamp")


          // 分箱
          val bucketizer = new Bucketizer()
            .setInputCol("periodTimeStamp")
            .setOutputCol(bucket)
            .setSplits(splitsBC.value)
          var bucketedData = bucketizer.transform(df_with_timeStamp)

          bucketedData = presentationFormat match {
            case "numericFormatWithNotID" => bucketedData
            case "numericFormatWithID" => throw new Exception("Sorry,该功能还未开发")
            case "timeFormatWithID" => {
              // 以时间显示
              val time_format_rdd = bucketedData.rdd.map(x => {
                val phasesTimeStamp = x(x.fieldIndex("phasesTimeStamp")).toString.toDouble
                val binningID = x(x.fieldIndex(bucket)).toString.toDouble
                val tmp = splitsBC.value(binningID.toInt) + phasesTimeStamp
                val binningTime = formatDate(tmp.toLong, timeFormat)
                var array = x.toSeq.toBuffer
                array += binningTime
                Row.fromSeq(array)
              })


              val newDataDF = sqlc.createDataFrame(time_format_rdd, {
                val newSchemaFields = bucketedData.schema.fields
                var w = newSchemaFields.toBuffer
                w += StructField(bucket + "_timeFormat", StringType)
                StructType(w.toArray)})
              newDataDF
            }
            case "timeFormatWithNotID" => throw new Exception("Sorry,该功能还未开发")
          }

          bucketedData = bucketedData.drop("timeStamp")
          bucketedData = bucketedData.drop("periodTimeStamp")
          bucketedData
        }
      }

      new_df.show()

      /**
        * 输出
        */
      new_df.cache()
      outputrdd.put(rddTableName, new_df)
      new_df.registerTempTable(rddTableName)
      sqlc.cacheTable(rddTableName)




    }
}
