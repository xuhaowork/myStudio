package com.self.core.featurePretreatment.objects

import com.google.gson.{Gson, JsonParser}
import com.zzjz.deepinsight.basic.BaseMain
import com.zzjz.deepinsight.core.featurePretreatment.models.{TokenizerByRegex, TokenizerByRegexParamsName}
import org.apache.spark.sql.DataFrame


object FeaturePretreatment extends BaseMain {
  val data: DataFrame = {
    sqlc.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes, which i like mostly"),
      (1, "Does Logistic regression models has a implicit params, Halt?")
    )).toDF("label", "sentence")
  }
  val rddTableName = "<#zzjzRddName#>"

  data.cache()
  outputrdd.put(rddTableName, data)
  data.registerTempTable(rddTableName)
  data.sqlContext.cacheTable(rddTableName)

  override def run(): Unit = {
    /**
      * 一些参数的处理
      */
    /** 0)获取基本的系统变量 */
    val jsonparam = "<#zzjzParam#>"
    val gson = new Gson()
    val p: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])
    val z1 = z


    /** 1)获取DataFrame */
    val tableName = p.get("inputTableName").trim
    val rawDataFrame = z1.rdd(tableName).asInstanceOf[org.apache.spark.sql.DataFrame]
    val parser = new JsonParser()
    val pJsonParser = parser.parse(jsonparam).getAsJsonObject

    val pretreatmentObj = pJsonParser.getAsJsonObject("pretreatmentType")
    val pretreatmentType = pretreatmentObj.get("value").getAsString
    val newDataFrame = pretreatmentType match {
      case "attributesDataTransform" => { // 属性类特征转换  一个输入一个输出
        val inputCol = pretreatmentObj.get("inputCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
        val outputCol = pretreatmentObj.get("outputCol").getAsString

        val pretreatObj = pretreatmentObj.get("pretreatment").getAsJsonObject
        pretreatObj.get("value").getAsString match {
          case "TokenizerByRegex" => {
            val pattern = pretreatObj.get("pattern").getAsString
            val gaps = try {
              pretreatObj.get("gaps").getAsString == "true"
            } catch {
              case _: Exception => throw new Exception("是否转为小写，参数输入出现异常")
            }
            val minTokenLength = try {
              pretreatObj.get("minTokenLength").getAsString.toDouble.toInt
            } catch {
              case _: Exception => throw new Exception("您输入的最小分词数不能转为Int类型")
            }


            new TokenizerByRegex(rawDataFrame)
              .setParams(TokenizerByRegexParamsName.inputCol, inputCol)
              .setParams(TokenizerByRegexParamsName.outputCol, outputCol)
              .setParams(TokenizerByRegexParamsName.gaps, gaps)
              .setParams(TokenizerByRegexParamsName.minTokenLength, minTokenLength)
              .setParams(TokenizerByRegexParamsName.pattern, pattern)
              .run()
              .data
          }
          case "CountWordVector" => rawDataFrame
          case "HashTF" =>rawDataFrame
          case "WordToVector" =>rawDataFrame
          case "StopWordsRemover" =>rawDataFrame
          case "NGram" =>rawDataFrame
        }


      } // 一个输入一个输出
      case "numericDataTransform" => rawDataFrame// 数值类特征转换 一个输入一个输出
      case "numericScale" => rawDataFrame// 数量尺度变换
      case "attributesWithNumeric" => rawDataFrame// 数值类型和属性类型互转
      case "featureSelect" => rawDataFrame// 特征选择
    }

    newDataFrame.show()





  }
}
