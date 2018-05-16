package com.self.core.learningSQL

import com.self.core.baseApp.myAPP
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.catalyst.trees


/**
  * Created by DataShoe on 2018/1/30.
  */
object LearningSQL extends myAPP{
  override def run(): Unit = {
    val arr = List(
      Array("1", "00:00:00"),
      Array("1", "01:2:10"),
      Array("1", "02:2:10"),
      Array("1", "01:2:10"),
      Array("1", "3:2:10"),
      Array("1", "5:2:10"),
      Array("1", "00:2:10"),
      Array("1", "01:59:10"),
      Array("1", null),
      Array("1", "10:2:32"),
      Array("1", "23:00:10"),
      Array("1", "A"))

    val rdd = sc.parallelize(arr).map(Row.fromSeq(_))
    var df = sqlc.createDataFrame(rdd, StructType(Array(StructField("id", StringType), StructField("dayTime", StringType))))

    df = df.withColumn("gmtStart", lit("1970-01-01"))

    val paste = udf((s1: String, s2: String) => s1 + "," + s2)
    df = df
      .withColumn("binnerTime", paste(col("gmtStart"), col("dayTime")))
      .withColumn("binnerStampTime", unix_timestamp(col("binnerTime"), "yyyy-MM-dd HH:mm:ss").+(28800))
      .drop("gmtStart")
      .drop("binnerTime")

    val width = "3"
    val widthUnit = "h"
    val widthLength = widthUnit match {
      case "h" => util.Try(width.toInt).getOrElse(-1)*3600
      case "m" => util.Try(width.toInt).getOrElse(-1)*60
      case "s" => util.Try(width.toInt).getOrElse(-1)*1
    }

    require(widthLength > 0, "您输入的分箱间隔应该为数值，并且需要选择对应分箱单位。")


// SparkSQL automatically has null transformation, though it is limited.
    val binner2Time = udf((s1: Long) => s1 /widthLength * widthLength)
    df.withColumn("binner_", binner2Time(col("binnerStampTime"))).show()



    // method1
//    val binner2Time = udf((s1: Long) => s1 /widthLength * widthLength)
//
//    df.withColumn("binner_", when(col("binnerStampTime").isNull, lit(null))
//      .otherwise(binner2Time(col("binnerStampTime")))).show()

    // method2 -- work
//    val binner2Time = udf((s1: Long) => {
//      val s: Option[Long] = util.Try(s1 /widthLength * widthLength).toOption
//      s
//    })
//
//    df.withColumn("binner_", binner2Time(col("binnerStampTime"))).show()


//    // method3
//    /**
//      * Set of methods to construct [[org.apache.spark.sql.UserDefinedFunction]]s that
//      * handle `null` values.
//      */
//    object NullableFunctions {
//
//      import org.apache.spark.sql.functions._
//      import scala.reflect.runtime.universe.{TypeTag}
//      import org.apache.spark.sql.UserDefinedFunction
//
//      /**
//        * Given a function A1 => RT, create a [[org.apache.spark.sql.UserDefinedFunction]] such that
//        *   * if fnc input is null, None is returned. This will create a null value in the output Spark column.
//        *   * if A1 is non null, Some( f(input) will be returned, thus creating f(input) as value in the output column.
//        * @param f function from A1 => RT
//        * @tparam RT return type
//        * @tparam A1 input parameter type
//        * @return a [[org.apache.spark.sql.UserDefinedFunction]] with the behaviour describe above
//        */
//      def nullableUdf[RT: TypeTag, A1: TypeTag](f: Function1[A1, RT]): UserDefinedFunction = {
//        udf[Option[RT],A1]( (i: A1) => i match {
//          case null => None
//          case s => Some(f(i))
//        })
//      }
//
//      /**
//        * Given a function A1, A2 => RT, create a [[org.apache.spark.sql.UserDefinedFunction]] such that
//        *   * if on of the function input parameters is null, None is returned.
//        *     This will create a null value in the output Spark column.
//        *   * if both input parameters are non null, Some( f(input) will be returned, thus creating f(input1, input2)
//        *     as value in the output column.
//        * @param f function from A1 => RT
//        * @tparam RT return type
//        * @tparam A1 input parameter type
//        * @tparam A2 input parameter type
//        * @return a [[org.apache.spark.sql.UserDefinedFunction]] with the behaviour describe above
//        */
//      def nullableUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag](f: Function2[A1, A2, RT]): UserDefinedFunction = {
//        udf[Option[RT], A1, A2]( (i1: A1, i2: A2) =>  (i1, i2) match {
//          case (null, _) => None
//          case (_, null) => None
//          case (s1, s2) => Some((f(s1,s2)))
//        } )
//      }
//    }
//
//    val binner2Time = NullableFunctions.nullableUdf((s1: Long) => s1 /widthLength * widthLength)
//
//    df.withColumn("binner_", binner2Time(col("binnerStampTime"))).show()






  }
}
