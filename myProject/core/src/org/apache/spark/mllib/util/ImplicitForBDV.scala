package org.apache.spark.mllib.util

import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM}

object ImplicitForBDV {
  implicit class BDVLast(var value: BDV[Double]){
    def head(n: Int): BDV[Double] = {
      new BDV[Double](value.data.dropRight(value.length - n))
      this.value =
    }
    def last(n: Int): BDV[Double] = new BDV[Double](value.data.drop(value.length - n))

    def toLowTriangle = {
      val num = scala.math.sqrt(1 + 8 * value.length)
      require(num.toInt >= num && num.toInt % 2 == 0, "您输入的向量长度不能转为下三角类型。")
      val dim = (-1 + num.toInt) /2




    }
  }


}
