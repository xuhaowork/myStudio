package com.self.core.probitRegresson

import com.google.gson.{Gson, JsonParser}
import com.zzjz.deepinsight.basic.BaseMain
import org.apache.spark.mllib.classification.Probit
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * Probit回归算子
  * ----
  * 解决二元probit回归的问题
  * ----
  * 注意：
  * 1.会将大于0的转为1，将小于等于0的数转为0
  * 2.会将不能转为0的string等类型转为null并过滤掉
  */

/**
  * editor: xuhao
  * date: 2018-05-15 10:30:00
  */

object ProbitRegression extends BaseMain {
  override def run(): Unit = {
    /**
      * 一些参数的处理
      */
    /** 0)获取基本的系统变量 */
    val jsonparam = "<#zzjzParam#>"
    val gson = new Gson()
    val p: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])
    val z1 = z
    val rddTableName = "<#zzjzRddName#>"

    /** 1)获取DataFrame */
    val tableName = p.get("inputTableName").trim
    val rawDataDF = z1.rdd(tableName).asInstanceOf[org.apache.spark.sql.DataFrame]
    val parser = new JsonParser()
    val pJsonParser = parser.parse(jsonparam).getAsJsonObject

    /** 2)获取对应的特征列名 */
    var featuresSchema = ArrayBuffer.empty[(String, String)]
    val featuresArr = pJsonParser.getAsJsonArray("features")
    for (i <- 0 until featuresArr.size()) {
      val featureObj = featuresArr.get(i).getAsJsonObject
      val tup = (featureObj.get("name").getAsString, featureObj.get("datatype").getAsString)
      featuresSchema += tup
    }

    /** 3)获取对应的标签类名信息 */
    val labelObj = pJsonParser.getAsJsonArray("label").get(0).getAsJsonObject
    val (labelName, labelDataType) = (labelObj.get("name").getAsString, labelObj.get("datatype").getAsString)

    /** 4)数据转换 */
    val trainData: RDD[LabeledPoint] = rawDataDF.rdd.map(row => {
      val arr = featuresSchema.map {
        case (name, dataType) =>
          dataType match {
            case "string" => row.getAs[String](name).toDouble
            case "int" => row.getAs[Int](name).toDouble
            case "double" => row.getAs[Double](name)
            case "float" => row.getAs[Float](name).toDouble
            case "long" => row.getAs[Long](name).toDouble
            case "boolean" => if (row.getAs[Boolean](name)) 1.0 else 0.0
            case _ => throw new Exception(
              "目前支持string、int、double、float、long以及boolean类型的特征字段")
          }
      }.toArray

      val label = labelDataType match {
        case "string" => row.getAs[String](labelName).toDouble
        case "int" => row.getAs[Int](labelName).toDouble
        case "double" => row.getAs[Double](labelName)
        case "float" => row.getAs[Float](labelName).toDouble
        case "long" => row.getAs[Long](labelName).toDouble
        case "boolean" => if (row.getAs[Boolean](labelName)) 1.0 else 0.0
        case _ => throw new Exception(
          "目前支持string、int、double、float、long以及boolean类型的特征字段")
      }

      LabeledPoint(if (label > 0) 1.0 else 0.0, new DenseVector(arr))
    })


    /** 数据处理 */
    val optimizationOptionObj = pJsonParser.getAsJsonObject("optimizationOption")
    val optimizationOption = optimizationOptionObj.get("value").getAsString
    val probitModel = optimizationOption match {
      case "SGD" =>
        val numIterations: Int = try {
          val numString = optimizationOptionObj.get("numIterations")
          if (numString.eq(null)) 200 else numString.getAsString.toInt
        } catch {
          case failure: Exception => throw new Exception(s"没有找到最大迭代次数的信息, $failure")
        }

        val stepSize: Double = try {
          val stepSizeString = optimizationOptionObj.get("stepSize")
          val learningRate = if (stepSizeString.eq(null)) 1.0 else stepSizeString.getAsString.toDouble
          require(learningRate <= 1.0 && learningRate >= 0.0, "学习率需要在0到1中间")
          learningRate
        } catch {
          case failure: Exception => throw new Exception(s"学习率信息异常, $failure")
        }

        val miniBatchFraction: Double = try {
          val stepSizeString = optimizationOptionObj.get("miniBatchFraction")
          val fraction = if (stepSizeString.eq(null)) 1.0 else stepSizeString.getAsString.toDouble
          require(fraction <= 1.0 && fraction >= 0.0, "随机批次下降占比需要在0到1中间")
          fraction
        } catch {
          case failure: Exception => throw new Exception(s"梯度下降批次信息异常$failure")
        }

        val addIntercept = try {
          if (optimizationOptionObj.get("addIntercept").getAsString == "true")
            true
          else
            false
        } catch {
          case failure: Exception => throw new Exception(s"截距信息没有填写, $failure")
        }

        Probit.trainWithSGD(trainData, 2, numIterations, stepSize,
          miniBatchFraction, addIntercept)

      case "LBFGS" =>
        val addIntercept = try {
          if (optimizationOptionObj.get("addIntercept").getAsString == "true")
            true
          else
            false
        } catch {
          case _: Exception => throw new Exception("截距信息没有获得")
        }
        Probit.trainWithLBFGS(trainData, 2, addIntercept)
    }

    val probitModelBC = rawDataDF.sqlContext.sparkContext.broadcast(probitModel).value

    val resultRdd = trainData.map(labeledPoint =>
      labeledPoint.features.toDense.values
        :+ labeledPoint.label
        :+ probitModelBC.predict(labeledPoint.features)).map(Row.fromSeq(_))

    val schema = featuresSchema.map(s => StructField(s._1, DoubleType)).toArray

    var newDataDF = rawDataDF.sqlContext.createDataFrame(resultRdd,
      StructType(schema :+ StructField(labelName, DoubleType)
        :+ StructField(labelName + "_fit", DoubleType)))

    for (each <- featuresSchema) {
      newDataDF = newDataDF.withColumn(each._1, col(each._1).cast(each._2))
    }

    newDataDF = newDataDF.withColumn(labelName, col(labelName).cast(labelDataType))
      .withColumn(labelName + "_fit", col(labelName + "_fit").cast(labelDataType))


    /** 打印参数信息 */
    println("coefficient:")
    println(probitModel.weights.toDense.values.mkString(", "))
    println("intercept:")
    println(probitModel.intercept)


    /** 输出结果 */
    newDataDF.cache()
    outputrdd.put(rddTableName, newDataDF)
    newDataDF.registerTempTable(rddTableName)
    newDataDF.sqlContext.cacheTable(rddTableName)


  }
}
