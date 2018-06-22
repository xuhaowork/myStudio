package com.self.core.treeEnsembleModelPredict.tests

import com.zzjz.deepinsight.basic.BaseMain
import com.zzjz.deepinsight.core.featurePretreatment.utils.Tools
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.{DataFrame, Row, UserDefinedFunction}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object testTreeEnsemblePredict extends BaseMain {
  def simulate(): DecisionTreeModel = {
    val rawDataFrame = sqlc.createDataFrame(
      Array(
        (0.0, 1, 32f, Vectors.dense(Array(1, 2, 0).map(_.toDouble))),
        (1.0, 1, 27f, Vectors.dense(Array(0, 1, 1).map(_.toDouble))),
        (1.0, 1, 22f, Vectors.dense(Array(0, 1, 0).map(_.toDouble))),
        (1.0, 1, 25f, Vectors.dense(Array(1, 1, 1).map(_.toDouble))),
        (0.0, 1, 23f, Vectors.dense(Array(0, 2, 1).map(_.toDouble)))
      )
    ).toDF("label", "intType", "floatType", "feature")
    outputrdd.put("tableName", rawDataFrame)

    getModel
  }


  private def getModel = {
    val data1 = Array(
      (0.0, Vectors.dense(Array(32, 1, 1, 0).map(_.toDouble))),
      (0.0, Vectors.dense(Array(25, 1, 2, 0).map(_.toDouble))),
      (3.0, Vectors.dense(Array(29, 1, 2, 1).map(_.toDouble))),
      (1.0, Vectors.dense(Array(24, 1, 1, 0).map(_.toDouble))),
      (3.0, Vectors.dense(Array(31, 1, 1, 0).map(_.toDouble))),
      (2.0, Vectors.dense(Array(35, 1, 2, 1).map(_.toDouble))),
      (0.0, Vectors.dense(Array(30, 0, 1, 0).map(_.toDouble))),
      (3.0, Vectors.dense(Array(31, 1, 1, 0).map(_.toDouble))),
      (1.0, Vectors.dense(Array(30, 1, 2, 1).map(_.toDouble))),
      (2.0, Vectors.dense(Array(21, 1, 1, 0).map(_.toDouble))),
      (0.0, Vectors.dense(Array(21, 1, 2, 0).map(_.toDouble))),
      (1.0, Vectors.dense(Array(21, 1, 2, 1).map(_.toDouble))),
      (3.0, Vectors.dense(Array(29, 0, 2, 1).map(_.toDouble))),
      (0.0, Vectors.dense(Array(29, 1, 0, 1).map(_.toDouble))),
      (2.0, Vectors.dense(Array(29, 0, 2, 1).map(_.toDouble))),
      (1.0, Vectors.dense(Array(30, 1, 1, 0).map(_.toDouble)))
    )

    val rawDataFrame1 = sqlc.createDataFrame(data1).toDF("label", "feature")

    def transform(rawDataFrame1: DataFrame): RDD[LabeledPoint] = {
      rawDataFrame1.select("label", "feature").mapPartitions(rowPartition =>
        rowPartition.filter(row => !row.isNullAt(0) && !row.isNullAt(1)).map(row => {
          val label = row.get(0) match {
            case d: Double => d
            case i: Int => i.toDouble
          }
          val feature = row.get(1) match {
            case dv: DenseVector => dv
            case sv: SparseVector => sv.toDense
          }

          LabeledPoint(label, feature)
        })
      )
    }


    val trainData: RDD[LabeledPoint] = transform(rawDataFrame1)
    //分类
    val numClasses = 4
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"

    //最大深度
    val maxDepth = 5
    //最大分支
    val maxBins = 32

    //模型训练
    DecisionTree.trainClassifier(trainData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)
  }


  override def run(): Unit = {
    val model = simulate()


    /**
      * 一些参数的处理
      */
    /** 0)获取基本的系统变量 */
    //    val jsonparam = "<#zzjzParam#>"
    //    val gson = new Gson()
    //    val p: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])
    //    val parser = new JsonParser()
    //    val pJsonParser = parser.parse(jsonparam).getAsJsonObject
    //    val z1 = z
    //    val rddTableName = "<#zzjzRddName#>"

    /** 1)获取DataFrame */
    val tableName = "tableName"
    //      try {
    //      p.get("inputTableName").trim
    //    } catch {
    //      case e: Exception => throw new Exception(s"没有找到您输入的预测表参数，具体信息${e.getMessage}")
    //    }

    val rawDataFrame = try {
      outputrdd.get(tableName).asInstanceOf[org.apache.spark.sql.DataFrame]
    } catch {
      case e: Exception => throw new Exception(s"获取预测表${tableName}过程中失败，具体信息${e.getMessage}")
    }

    /** 2)请输入特征列名 */
    //    val featureColsArr = pJsonParser.get("featureCols").getAsJsonArray
    val featureCols = Array("floatType", "feature")


    /** 3)读取模型 */

    import org.apache.spark.mllib.tree.configuration.Algo.Classification

    require(model.algo == Classification, s"概率形式输出要求模型必须为分类模型。您的类型为${model.algo.toString}")

    /** 3)udf预测函数 */

    val naType = "skip" // "error", "skip"


    import com.zzjz.deepinsight.core.treeEnsembleModelPredict.models.ImplicitForDecisionTree._
    def materialForUDF(predict: String, naType: String = naType): UserDefinedFunction = udf {
      (row: Row) =>
        val arr = mutable.ArrayBuilder.make[Double]
        var noNA = true

        /** 获取Row对应所有的元素 */
        row.toSeq.foreach {
          a => {
            a match {
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
        arr.result()

//        val prediction = if (predict == "value") {
//          model.predictWithProb(Vectors.dense(arr.result()))._1
//        } else {
//          model.predictWithProb(Vectors.dense(arr.result()))._2
//        }
//        naType match {
//          case "error" => if (noNA) Some(prediction) else
//            throw new Exception("您输入的数据中有缺失值或者double的NaN值")
//          case "skip" => if (noNA) Some(prediction) else None
//        }
    }


    val predictValue = materialForUDF("value")
    val predictProb = materialForUDF("prob")

    /** 4)预测 */
    val predictValueCol = "predictValueCol"

    require(!Tools.columnExists(predictValueCol, rawDataFrame, false), s"您的输出列名${predictValueCol}已经存在")

    val predictProbCol = "predictProbCol"
    require(!Tools.columnExists(predictValueCol, rawDataFrame, false), s"您的输出列名${predictProbCol}已经存在")

    val newDataFrame = rawDataFrame.select(
      col("*"),
      predictValue(struct(featureCols.map(s => col(s)): _*)).as(predictValueCol),
      predictProb(struct(featureCols.map(s => col(s)): _*)).as(predictProbCol)
    )

    newDataFrame.show()

    import scala.collection.mutable.ArrayBuffer
    val buffer = new ArrayBuffer[Double]()
    buffer += 1.0

  }
}
