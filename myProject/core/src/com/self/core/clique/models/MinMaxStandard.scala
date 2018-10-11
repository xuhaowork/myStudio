package com.self.core.clique.models

import org.apache.spark.sql.functions.{col, max, min, struct}
import org.apache.spark.sql.{DataFrame, NullableFunctions, Row, UserDefinedFunction}

/**
  * 数据标准化模型
  *
  * @param minRes   最小值
  * @param maxRes   最大值
  * @param features 特征
  * @define standardize   标准化
  * @define unStandardize 反标准化
  */
class MinMaxStandardModel(val minRes: Seq[Double], val maxRes: Seq[Double], val features: Array[String]) extends Serializable {
  var standardCol: String = "standardCol"
  var unStandardCol: String = "unStandardCol"

  def setStandardCol(standardCol: String): this.type = {
    this.standardCol = standardCol
    this
  }

  def setUnStandardCol(unStandardCol: String): this.type = {
    this.unStandardCol = unStandardCol
    this
  }

  def standardize(data: DataFrame): DataFrame = {
    val standard: UserDefinedFunction = NullableFunctions.udf(
      (row: Row) =>
        row.toSeq.toArray.map(_.toString.toDouble).zip(minRes).zip(maxRes).map {
          case ((value, minR), maxR) =>
            if (minR >= maxR)
              0.0
            else
              (value - minR) / (maxR - minR)
        }
    )

    data.withColumn(standardCol, standard(struct(features.map(col): _*)))
  }

  def unStandardize(data: DataFrame): DataFrame = {
    val unStandard: UserDefinedFunction = NullableFunctions.udf(
      (values: Seq[Double]) =>
        values.toArray.zip(minRes).zip(maxRes).map {
          case ((value, minR), maxR) =>
            if (minR >= maxR)
              minR
            else
              value * (maxR - minR) + minR
        }
    )
    data.withColumn(unStandardCol, unStandard(col(standardCol)))
  }

}

/** 标准化模型 */
object MinMaxStandard {
  def train(data: DataFrame, features: Array[String]): MinMaxStandardModel = {
    val row = data.select(features.map(name => min(col(name))) ++ features.map(name => max(col(name))): _*).head()
    val (minRes: Seq[Double], maxRes: Seq[Double]) = row.toSeq.map(_.toString.toDouble).splitAt(features.length)

    new MinMaxStandardModel(minRes, maxRes, features)
  }

}
