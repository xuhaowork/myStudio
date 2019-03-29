package cn.datashoe.examples

import org.scalatest.FunSuite

class SparkAPISuite extends FunSuite {
  test("spark window函数") {
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.{SparkConf, SparkContext}
    import org.joda.time.DateTime

    val conf = new SparkConf().setAppName("Window Functions").setMaster("local")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val sqlc = sparkSession.sqlContext

    import org.apache.spark.sql.functions._

    val l = (1997, 1) :: (1997, 4) :: (1998, 2) :: (1998, 3) :: (1999, 9) :: Nil

    val df = sqlc.createDataFrame(l).toDF("k", "v").repartition(col(""))

    println("原数据")
    df.show()

    // 原始分区
    // [1997,4]
    // [1998,2], [1997,1]
    //
    // [1999,9], [1998,3]
    //
    //    df.foreachPartition {
    //      a =>
    //        println(a.mkString(", "))
    //    }
    //
    //
    val df1 = df.repartition(col("k"))
    //
    //    val pairRdd = sc.parallelize(Seq((1, 2), (3, 4)))


    //    +----+---+
    //    |   k|  v|
    //    +----+---+
    //    |1997|  1|
    //    |1997|  4|
    //    |1998|  2|
    //    |1998|  3|
    //    |1999|  9|
    //    +----+---+

    //    val w = Window.partitionBy("")
    //    val df1 = df.withColumn("No", row_number().over(w))

    //    sum(x^2) - count * avg(x)^2
    //    count sum(x), sum(x ^ 2)
    //    sum(x ^ 2) - sum(x) ^ 2 / count
    //    1, x, x^2
    //
    //    udaf


    //    df1.rdd.mapPartitionsWithIndex {
    //      case (i, rows) =>
    //        Array((i, rows.mkString(", "))).toIterator
    //    }.foreach(println)

    println(new DateTime(2004, 1, 1, 0, 0, 0).getMillis)


    // 一班 姓名 乘积

    //    percent_rank().over(Window.orderBy("姓名").partitionBy("班级"))
    //    row_number().over(Window.orderBy("姓名").partitionBy("班级"))
    //    avg("").over()
    //
    //
    //    val u1 = Array.range(0, 1 << 31).map {
    //      i =>
    //        (i, "A")
    //    }
    //
    //    val u2 = Array.range(0, 1 << 31).map {
    //      i =>
    //
    //    }


    //    +----+---+---+------------------+
    //    |   k|  v| No|               row|
    //    +----+---+---+------------------+
    //    |1997|  1|  1|               1.0|
    //    |1997|  4|  2|               2.5|
    //    |1998|  2|  3|2.3333333333333335|
    //    |1998|  3|  4|               3.0|
    //    |1999|  9|  5| 4.666666666666667|
    //    +----+---+---+------------------+


    //    val rowW = w.rowsBetween(-2, 0)
    //    val rangeW = w.rangeBetween(-1, 0)
    //
    //    val res = df1.withColumn("row", avg($"v").over(rowW))
    //    res.show()
    ////      .withColumn("range", avg($"v").over(rangeW)).show
    //    sc.stop()

  }

}
