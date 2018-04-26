package com.self.core.VAR

import org.apache.commons.math3.analysis.interpolation.SplineInterpolator
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Vector => BV}


/**
  * 按行或列对breeze的DenseVector、DenseMatrix进行缺失值填充
  * ----
  * 1）对breeze的DenseVector的缺失值进行补全
  * 2）对breeze的DenseMatrix类型的元素进行缺失值补全（稀疏矩阵当然要变为DenseMatrix才能补全）
  */
object ColNAFill {

  /**
    * @param arr 为二维向量，默认从外道第一维为列，第二维为行
    *            即："Array[ Array[Double] ]"里面的Array[Double]代表列, storedByCols为true即数据按列储存
    */
  implicit class ArrayToDenseMatrix(val arr: Array[Array[Double]]){
    def toDenseMatrix(storedByCols: Boolean = true): BDM[Double] = {
      val cols = arr.length
      val rows = arr.head.length
      if(storedByCols)
        new BDM(rows, cols, arr.flatten)
      else
        new BDM(rows, cols, arr.flatten, 0, cols, true)
    }
  }

  implicit class DenseMatrixToArrayArray(val bm: BDM[Double]){
    def toArrayArray: Array[Array[Double]] =
      if(bm.isTranspose)
        Array.range(0, bm.size, bm.rows).map(start => bm.data.slice(start, start + bm.rows)).transpose
      else
        Array.range(0, bm.size, bm.rows).map(start => bm.data.slice(start, start + bm.rows))

  }



  def fillByCols(bm: BDM[Double], fillMethod: String): BDM[Double] =
    fillByCols(bm.toArrayArray, fillMethod)


  def fillByRows(bm: BDM[Double], fillMethod: String): BDM[Double] =
    fillByRows(bm.toArrayArray, fillMethod)



  /**
    *
    * @param values
    * @param fillMethod
    * @param isTranspose 是否转置
    * @return
    */
  def fillByCols(values: Array[Array[Double]], fillMethod: String, isTranspose: Boolean = false): BDM[Double] = {
    val arr = if(isTranspose) values.transpose else values
    arr.map(fillArray(_, fillMethod)).toDenseMatrix()
  }

  def fillByRows(values: Array[Array[Double]], fillMethod: String, isTranspose: Boolean = false): BDM[Double] = {
    val arr = if(!isTranspose) values.transpose else values
    arr.map(fillArray(_, fillMethod)).toDenseMatrix()
  }



  def fillArray(ts: Array[Double], fillMethod: String): Array[Double] = {
    fillMethod match {
      case "linear" => fillLinear(ts)
      case "nearest" => fillNearest(ts)
      case "next" => fillNext(ts)
      case "previous" => fillPrevious(ts)
      case "spline" => fillSpline(ts)
      case "zero" => fillValue(ts, 0)
      case _ => throw new UnsupportedOperationException()
    }
  }


  /**
    * Replace all NaNs with a specific value
    */
  def fillValue(values: Array[Double], filler: Double): Array[Double] = {
    fillValue(new BDV[Double](values), filler).toArray
  }

  /**
    * Replace all NaNs with a specific value
    */
  def fillValue(values: BV[Double], filler: Double): BDV[Double] = {
    val result = values.copy.toArray
    var i = 0
    while (i < result.size) {
      if (result(i).isNaN) result(i) = filler
      i += 1
    }
    new BDV[Double](result)
  }

  def fillNearest(values: Array[Double]): Array[Double] = {
    fillNearest(new BDV[Double](values)).toArray
  }

  def fillNearest(values: BV[Double]): BDV[Double] = {
    val result = values.copy.toArray
    var lastExisting = -1
    var nextExisting = -1
    var i = 1
    while (i < result.length) {
      if (result(i).isNaN) {
        if (nextExisting < i) {
          nextExisting = i + 1
          while (nextExisting < result.length && result(nextExisting).isNaN) {
            nextExisting += 1
          }
        }

        if (lastExisting < 0 && nextExisting >= result.length) {
          throw new IllegalArgumentException("Input is all NaNs!")
        } else if (nextExisting >= result.length || // TODO: check this
          (lastExisting >= 0 && i - lastExisting < nextExisting - i)) {
          result(i) = result(lastExisting)
        } else {
          result(i) = result(nextExisting)
        }
      } else {
        lastExisting = i
      }
      i += 1
    }
    new BDV[Double](result)
  }

  def fillPrevious(values: Array[Double]): Array[Double] = {
    fillPrevious(new BDV[Double](values)).toArray
  }

  /**
    * fills in NaN with the previously available not NaN, scanning from left to right.
    * 1 NaN NaN 2 Nan -> 1 1 1 2 2
    */
  def fillPrevious(values: BV[Double]): BDV[Double] = {
    val result = values.copy.toArray
    var filler = Double.NaN // initial value, maintains invariant
    var i = 0
    while (i < result.length) {
      filler = if (result(i).isNaN) filler else result(i)
      result(i) = filler
      i += 1
    }
    new BDV[Double](result)
  }

  def fillNext(values: Array[Double]): Array[Double] = {
    fillNext(new BDV[Double](values)).toArray
  }

  /**
    * fills in NaN with the next available not NaN, scanning from right to left.
    * 1 NaN NaN 2 Nan -> 1 2 2 2 NaN
    */
  def fillNext(values: BV[Double]): BDV[Double] = {
    val result = values.copy.toArray
    var filler = Double.NaN // initial value, maintains invariant
    var i = result.length - 1
    while (i >= 0) {
      filler = if (result(i).isNaN) filler else result(i)
      result(i) = filler
      i -= 1
    }
    new BDV[Double](result)
  }

  def fillWithDefault(values: Array[Double], filler: Double): Array[Double] = {
    fillWithDefault(new BDV[Double](values), filler).toArray
  }

  /**
    * fills in NaN with a default value
    */
  def fillWithDefault(values: BV[Double], filler: Double): BDV[Double] = {
    val result = values.copy.toArray
    var i = 0
    while (i < result.length) {
      result(i) = if (result(i).isNaN) filler else result(i)
      i += 1
    }
    new BDV[Double](result)
  }

  def fillLinear(values: Array[Double]): Array[Double] = {
    fillLinear(new BDV[Double](values)).toArray
  }

  def fillLinear(values: BV[Double]): BDV[Double] = {
    val result = values.copy.toArray
    var i = 1
    while (i < result.length - 1) {
      val rangeStart = i
      while (i < result.length - 1 && result(i).isNaN) {
        i += 1
      }
      val before = result(rangeStart - 1)
      val after = result(i)
      if (i != rangeStart && !before.isNaN && !after.isNaN) {
        val increment = (after - before) / (i - (rangeStart - 1))
        for (j <- rangeStart until i) {
          result(j) = result(j - 1) + increment
        }
      }
      i += 1
    }
    new BDV[Double](result)
  }

  def fillSpline(values: Array[Double]): Array[Double] = {
    fillSpline(new BDV[Double](values)).toArray
  }

  /**
    * Fill in NaN values using a natural cubic spline.
    * @param values Vector to interpolate
    * @return Interpolated vector
    */
  def fillSpline(values: BV[Double]): BDV[Double] = {
    val result = values.copy.toArray
    val interp = new SplineInterpolator()
    val knotsAndValues = values.toArray.zipWithIndex.filter(!_._1.isNaN)
    // Note that the type of unzip is missed up in scala 10.4 as per
    // https://issues.scala-lang.org/browse/SI-8081
    // given that this project is using scala 10.4, we cannot use unzip, so unpack manually
    val knotsX = knotsAndValues.map(_._2.toDouble)
    val knotsY = knotsAndValues.map(_._1)
    val filler = interp.interpolate(knotsX, knotsY)

    // values that we can interpolate between, others need to be filled w/ other function
    var i = knotsX(0).toInt
    val end = knotsX.last.toInt

    while (i < end) {
      result(i) = filler.value(i.toDouble)
      i += 1
    }
    new BDV[Double](result)
  }




}
