package com.self.core.naFill

import com.self.core.baseApp.myAPP
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Vector => BV}


object NAFill extends myAPP{
  override def run(): Unit = {
    def fillLinear(values: BV[Double]): BDV[Double] = {
      val result = values.copy.toArray
      var i = 1
      while (i < result.length - 1) {
        val rangeStart = i
        while (i < result.length - 1 && result(i).isNaN) {
          i += 1
        }
        val before = result(rangeStart - 1)
        val after = if(result(i).isNaN) before else result(i)
        if (i != rangeStart && !before.isNaN) {
          val increment = (after - before) / (i - (rangeStart - 1))
          for (j <- rangeStart until i) {
            result(j) = result(j - 1) + increment
          }
        }
        i += 1
      }
      new BDV[Double](result)
    }




















  }
}
