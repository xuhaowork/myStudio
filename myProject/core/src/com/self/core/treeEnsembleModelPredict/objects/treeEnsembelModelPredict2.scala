package com.self.core.treeEnsembleModelPredict.objects

import com.google.gson.{Gson, JsonParser}
import com.zzjz.deepinsight.basic.BaseMain
import com.zzjz.deepinsight.core.featurePretreatment.utils.Tools
import com.zzjz.deepinsight.core.treeEnsembleModelPredict.models.ImplicitForDecisionTree._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField}


object treeEnsembelModelPredict2 extends BaseMain {
  override def run(): Unit = {
    /**
      * 一些参数的处理
      */
    /** 0)获取基本的系统变量 */
    val jsonparam = "<#zzjzParam#>"
    val gson = new Gson()
    val p: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])
    val parser = new JsonParser()
    val pJsonParser = parser.parse(jsonparam).getAsJsonObject
    val z1 = z
    val rddTableName = "<#zzjzRddName#>"

    /** 1)获取DataFrame */
    val tableName = try {
      p.get("inputTableName").trim
    } catch {
      case e: Exception => throw new Exception(s"没有找到您输入的预测表参数，具体信息${e.getMessage}")
    }

    val rawDataFrame = try {
      z1.rdd(tableName).asInstanceOf[org.apache.spark.sql.DataFrame]
    } catch {
      case e: Exception => throw new Exception(s"获取预测表${tableName}过程中失败，具体信息${e.getMessage}")
    }

    /** 2)请输入特征列名 */
    val featureColsArr = pJsonParser.get("featureCols").getAsJsonArray
    val featureCols = Array.range(0, featureColsArr.size()).map { i =>
      val obj = featureColsArr.get(i).getAsJsonObject
      val name = obj.get("name").getAsString
      Tools.columnTypesIn(name, rawDataFrame, Array("string", "double", "int", "float", "long", "vector"), true)
      name
    }


    /** 3)读取模型 */
    val predictTableName = try {
      p.get("predictTableName").trim
    } catch {
      case e: Exception => throw new Exception(s"没有找到您输入的模型信息参数，具体信息${e.getMessage}")
    }

    val predictTable = try {
      z1.rdd(predictTableName).asInstanceOf[org.apache.spark.sql.DataFrame]
    } catch {
      case e: Exception => throw new Exception(s"获取模型信息${tableName}过程中失败，具体信息${e.getMessage}")
    }

    val path = try {
      predictTable.select("path").head.getString(0)
    } catch {
      case e: Exception => throw new Exception(s"获取模型路径信息过程中失败，具体信息${e.getMessage}")
    }

    val model: DecisionTreeModel = try {
      rawDataFrame.sqlContext
        .sparkContext
        .broadcast(DecisionTreeModel.load(rawDataFrame.sqlContext.sparkContext, path))
        .value
    } catch {
      case e: Exception => throw new Exception(s"读取路径${path}中的决策树模型过程中失败，具体信息${e.getMessage}")
    }
    import org.apache.spark.mllib.tree.configuration.Algo.Classification

    require(model.algo == Classification, s"概率形式输出要求模型必须为分类模型。您的类型为${model.algo.toString}")


    /** 3)预测 */
    val predictValueCol = try {
      p.get("predictValueCol").trim
    } catch {
      case e: Exception => throw new Exception(s"您的输入预测值列名参数信息有误，具体信息${e.getMessage}")
    }

    require(!Tools.columnExists(predictValueCol, rawDataFrame, false), s"您的输出列名${predictValueCol}已经存在")

    val predictProbCol = try {
      p.get("predictProbCol").trim
    } catch {
      case e: Exception => throw new Exception(s"您的输入预测结点概率列名参数信息有误，具体信息${e.getMessage}")
    }
    require(!Tools.columnExists(predictValueCol, rawDataFrame, false), s"您的输出列名${predictProbCol}已经存在")


    val featureIdCols = featureCols.map(s => rawDataFrame.schema.fieldIndex(s))
    val newRdd = rawDataFrame.rdd.map(row => {
      val arr = featureIdCols.map(i => if (row.isNullAt(i)) Double.NaN else row.get(i).toString.toDouble)

      Row.merge(row, Row.fromTuple(model.predictWithProb(Vectors.dense(arr))))
    })

    val newSchema = rawDataFrame.schema.add(StructField("predictValue", DoubleType)).add(StructField("predictProb", DoubleType))

    val newDataFrame = rawDataFrame.sqlContext.createDataFrame(newRdd, newSchema)

    newDataFrame.show()


    newDataFrame.show()
    newDataFrame.registerTempTable(rddTableName)
    newDataFrame.sqlContext.cacheTable(rddTableName)
    outputrdd.put(rddTableName, newDataFrame)


  }
}
