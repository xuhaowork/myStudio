package com.self.core.treeEnsembleModelPredict.objects

import com.google.gson.{Gson, JsonParser}
import com.zzjz.deepinsight.basic.BaseMain
import com.zzjz.deepinsight.core.featurePretreatment.utils.Tools
import com.zzjz.deepinsight.core.treeEnsembleModelPredict.models.ImplicitForDecisionTree._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.{Row, UserDefinedFunction}

import scala.collection.mutable

/**
  * editor：Xuhao
  * date： 2017/6/22 10:00:00
  */


/**
  * 非常非常奇怪，这个脚本在平台上跑不起来，本地能跑起来
  * ----
  * 猜测：和udf有关，这里我用了一个udf接受了多个列作为参数。
  */
// @todo 后面弄一下

/**
  * 决策树模型预测节点概率输出
  *
  * 提供两个输出：一个决策树节点的预测值，一个节点对应类别的概率
  * ----注意这并不是软分类，最终预测类别仍然只有一个，只是输出了节点概率
  */
object treeEnsembleModelPredict extends BaseMain {
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

    /** 3)udf预测函数 */

    val naType = p.get("naType").trim // "error", "skip"


    def materialForUDF(predict: String, naType: String = naType): UserDefinedFunction = udf {
      (row: Row) =>
        val arr = mutable.ArrayBuilder.make[Double]
        var noNA = true

        /** 获取Row对应所有的元素 */
        row.toSeq.foreach {
          a => {
            a match {
              case s: String =>
                util.Try(s.toDouble).getOrElse {
                  noNA = false
                  Double.NaN
                }
              case d: Double =>
                arr += d
              case i: Int =>
                arr += i.toDouble
              case f: Float =>
                arr += f.toDouble
              case l: Long =>
                arr += l.toDouble
              case v: Vector =>
                v.toDense.values.foreach(
                  d =>
                    if (d.isNaN) {
                      noNA = false
                      arr += Double.NaN
                    } else
                      arr += d
                )
              case arrayDouble: Array[Double] =>
                arrayDouble.foreach(
                  d =>
                    if (d.isNaN) {
                      noNA = false
                      arr += Double.NaN
                    } else
                      arr += d
                )
              case null =>
                noNA = false
                Double.NaN
              case _ =>
                throw new Exception("您输入的训练列中有不支持的数据格式")
            }
          }
        }

        val prediction = if (predict == "value") {
          model.predictWithProb(Vectors.dense(arr.result()))._1
        } else {
          model.predictWithProb(Vectors.dense(arr.result()))._2
        }
        naType match {
          case "error" => if (noNA) Some(prediction) else
            throw new Exception("您输入的数据中有缺失值或者double的NaN值")
          case "skip" => if (noNA) Some(prediction) else None
        }
    }


    val predictValue = materialForUDF("value")
    val predictProb = materialForUDF("prob")

    /** 4)预测 */
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

    val newDataFrame = rawDataFrame.select(
      col("*"),
      predictValue(struct(featureCols.map(s => col(s)): _*)).as(predictValueCol),
      predictProb(struct(featureCols.map(s => col(s)): _*)).as(predictProbCol)
    )

    newDataFrame.show()


    newDataFrame.show()
    newDataFrame.registerTempTable(rddTableName)
    newDataFrame.sqlContext.cacheTable(rddTableName)
    outputrdd.put(rddTableName, newDataFrame)


  }

}
