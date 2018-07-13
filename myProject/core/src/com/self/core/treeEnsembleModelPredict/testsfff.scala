package com.self.core.treeEnsembleModelPredict

import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row

import scala.collection.mutable

object testsfff2 extends myAPP{
  override def run(): Unit = {

    val rawDataFrame = sqlc.createDataFrame(
      Array(
        (0.0, 1, 32f, Vectors.dense(Array(1, 2, 0).map(_.toDouble))),
        (1.0, 1, 27f, Vectors.dense(Array(0, 1, 1).map(_.toDouble))),
        (1.0, 1, 22f, Vectors.dense(Array(0, 1, 0).map(_.toDouble))),
        (1.0, 1, 25f, Vectors.dense(Array(1, 1, 1).map(_.toDouble))),
        (0.0, 1, 23f, Vectors.dense(Array(0, 2, 1).map(_.toDouble)))
      )
    ).toDF("label", "intType", "floatType", "feature")


    import org.apache.spark.sql.functions._

    val assemble = udf{ row: Row =>
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
      }



    rawDataFrame.withColumn("assemble", assemble(struct("intType", "floatType", "feature"))).show()





  }
}
