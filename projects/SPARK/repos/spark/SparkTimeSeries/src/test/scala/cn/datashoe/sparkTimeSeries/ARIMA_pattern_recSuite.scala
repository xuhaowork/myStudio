package cn.datashoe.sparkTimeSeries

import org.scalatest.FunSuite

class ARIMA_pattern_recSuite extends FunSuite {
  val data: Seq[Double] = Seq(
    -0.7645963,
    1.297196,
    0.6684268,
    -1.607385,
    -0.6260048,
    1.484327,
    1.885498,
    -0.009316681,
    -0.3396759,
    -0.9720703,
    -1.86799,
    0.8949122,
    2.638806,
    2.946696,
    2.877056,
    1.162571,
    0.9303286,
    1.72233,
    1.985159,
    2.427854,
    1.465286,
    3.264972,
    3.673337,
    4.362468,
    3.849653,
    2.203451,
    1.766208,
    2.045552,
    0.5108908,
    -0.8377184,
    1.127791,
    0.8509394,
    -0.5876151,
    0.04057314,
    -0.2745,
    1.841507,
    1.152422,
    -0.1830325,
    -0.0942581,
    -1.145601,
    -0.6118088,
    -1.565115,
    -1.195411,
    -1.210969,
    -1.199002,
    0.7432265,
    0.4647355,
    0.4655549,
    -0.8545023,
    -2.205812,
    -0.4594624,
    1.421927,
    1.168172,
    0.1715872,
    -0.1007282,
    0.5527776,
    2.700327,
    2.20518,
    2.847266,
    3.493271,
    2.81455,
    3.31633,
    2.027845,
    -0.06848588,
    -2.234106,
    -0.7367489,
    -2.218402,
    -3.164137,
    -0.6150999,
    1.007252,
    1.394324,
    -0.190602,
    2.186029,
    1.06044,
    -0.6892324,
    -0.04795455,
    0.3010811,
    -0.203175,
    -1.388942,
    -2.46916,
    -2.464309,
    -3.157809,
    -2.329074,
    -0.8525182,
    -1.896752,
    -0.7525859,
    0.6993575,
    0.8271095,
    -1.081455,
    -0.5862738,
    0.008037266,
    -0.743091,
    -0.7964282,
    -0.7613908,
    1.167228,
    0.8658421,
    0.4839877,
    -0.3398455,
    -0.8867167,
    -0.3751704
  )


  import org.apache.spark.mllib.linalg.Vector

  val ts: Vector = null

  val arMax = 7
  val maMax = 13

  implicit class VectorImpl(val ts: Array[Double]) {
    def lag(step: Int): Array[Double] = Array.fill(step)(Double.NaN) ++ ts.dropRight(1)

    def -(other: Array[Double]): Array[Double] = ts.zip(other).map { case (a, b) => a - b }

    def *(other: Array[Double]): Double = {
      var mul = 0.0
      for (i <- ts.indices) {
        mul += ts(i) * other(i)
      }
      mul
    }

    def *(multiplier: Double): Array[Double] = ts.map(_ * multiplier)


    def ::(other: Array[Double]): Array[Array[Double]] = {
      ts
    }

  }


  //    # 变为上三角矩阵
  //    reupm <- function(m1, nrow, ncol) {
  //      k <- ncol - 1
  //      m2 <- NULL
  //      for (i in 1:k) {
  //        i1 <- i + 1
  //        work <- lag1(m1[, i])
  //        work[1] <- -1
  //        temp <- m1[, i1] - work * m1[i1, i1]/m1[i, i]
  //        temp[i1] <- 0
  //        m2 <- cbind(m2, temp)
  //      }
  //      m2
  //    }


  /**
    * 转为上三角矩阵
    *
    * @param m 矩阵 Array(列) 按列存储
    */
  def reupm(m: Array[Array[Double]], nRows: Int, nCols: Int): Array[Array[Double]] =
    Array.tabulate(nCols - 1) {
      i =>
        val work = -1 +: m(i).dropRight(1)
        val tmp = m(i + 1) - work * (m(i + 1)(i + 1) / m(i)(i))
        tmp(i + 1) = 0.0
        tmp
    }


//  ceascf <- function(m, cov1, nar, ncol, count, ncov, z, zm) {
//    result <- 0 * seq(1, nar + 1)
//    result[1] <- cov1[ncov + count]
//    for (i in 1:nar) {
//      temp <- cbind(z[-(1:i)], zm[-(1:i), 1:i]) %*% c(1, -m[1:i, i])
//      result[i + 1] <- acf(temp, plot = FALSE, lag.max = count,
//        drop.lag.0 = FALSE)$acf[count + 1]
//    }
//    result
//  }

  def ceascf(m: Array[Array[Double]], cov: Array[Double], nar: Int, nCol: Double, nCov: Int, count: Int, ts: Array[Double], zm: Array[Array[Double]]) = {
    val res = new Array[Double](nar + 1)
    res(0) = cov(nCov + count - 1)
    val i = 1


    while(i < nar) {
      ts.takeRight(ts.length - i)

    }


  }


  test("测试") {


  }

}
