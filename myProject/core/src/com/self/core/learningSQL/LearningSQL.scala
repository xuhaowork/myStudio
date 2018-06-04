package com.self.core.learningSQL

import com.self.core.baseApp.myAPP
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.ml.feature._


/**
  * Created by DataShoe on 2018/1/30.
  */
object LearningSQL extends myAPP {
  override def run(): Unit = {
    //    val arr = List(
    //      Array("1", "00:00:00"),
    //      Array("1", "01:2:10"),
    //      Array("1", "02:2:10"),
    //      Array("1", "01:2:10"),
    //      Array("1", "3:2:10"),
    //      Array("1", "5:2:10"),
    //      Array("1", "00:2:10"),
    //      Array("1", "01:59:10"),
    //      Array("1", null),
    //      Array("1", "10:2:32"),
    //      Array("1", "23:00:10"),
    //      Array("1", "A"))
    //
    //    val rdd = sc.parallelize(arr).map(Row.fromSeq(_))
    //    var df: DataFrame = sqlc.createDataFrame(rdd, StructType(Array(StructField("id", StringType), StructField("dayTime", StringType))))
    //
    //    df = df.withColumn("gmtStart", lit("1970-01-01"))
    //
    //    sqlc.cacheTable("a")
    //
    //    val paste = udf((s1: String, s2: String) => s1 + "," + s2)
    //    df = df
    //      .withColumn("binnerTime", paste(col("gmtStart"), col("dayTime")))
    //      .withColumn("binnerStampTime", unix_timestamp(col("binnerTime"), "yyyy-MM-dd HH:mm:ss").+(28800))
    //      .drop("gmtStart")
    //      .drop("binnerTime")
    //
    //    val width = "3"
    //    val widthUnit = "h"
    //    val widthLength = widthUnit match {
    //      case "h" => util.Try(width.toInt).getOrElse(-1)*3600
    //      case "m" => util.Try(width.toInt).getOrElse(-1)*60
    //      case "s" => util.Try(width.toInt).getOrElse(-1)*1
    //    }
    //
    //    require(widthLength > 0, "您输入的分箱间隔应该为数值，并且需要选择对应分箱单位。")
    //
    //
    //// SparkSQL automatically has null transformation, though it is limited.
    //    val binner2Time = udf((s1: Long) => s1 /widthLength * widthLength)
    //    df.withColumn("binner_", binner2Time(col("binnerStampTime"))).show()
    //
    //    println(sqlc.tableNames().mkString(","))

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


    //    val lst = Array.fill(100)(1.0)
    //    val rdd2 = sc.parallelize(lst).map(Row(_))
    //    val newDataFrame = hqlc.createDataFrame(rdd2, StructType(Array(StructField("test", DoubleType))))
    //    newDataFrame.cache()
    //    outputrdd.put("<#zzjzRddName#>", newDataFrame)
    //    newDataFrame.registerTempTable("<#zzjzRddName#>")
    //    newDataFrame.sqlContext.cacheTable("<#zzjzRddName#>")

    println(sqlc.getConf("spark.sql.inMemoryColumnarStorage.compressed"))


    val schema = StructType(Seq(StructField("numbsToEValue", ArrayType(StringType, true)), StructField("lineToColValue", ArrayType(StructType(Seq(StructField("coorX", DoubleType, false), StructField("coorY", DoubleType, false)))), true)))

    /** 自定义一个DataType类型 */
    case class person(name: String, age: Int)
    class PersonType extends DataType {
      override def defaultSize: Int = 0

      override def asNullable: PersonType = this
    }

    case object PersonType extends PersonType
    val rdd = sc.parallelize(Seq(
      Row(0, person("Lina", 12)),
      Row(1, person("John", 51)),
      Row(2, person("Bill", 61))
    ))


    val df: DataFrame = sqlc.createDataFrame(rdd, StructType(Array(StructField("id", IntegerType), StructField("person", PersonType))))
    df.show()

    /** 探究一下DataFrame和Row等之间的关系 */
    val schema2 = StructType(
      Array(
        StructField("id", IntegerType),
        StructField("point", DoubleType)
      ))
    val rdd2 = sc.parallelize(Seq(
      Row(0, 0.1), // Row.apply生成一个GenericRow => new GenericRow(values.toArray)
      Row(1, 0.2),
      Row(2, 0.3)
    ))
    val df2: DataFrame = sqlc.createDataFrame(rdd2, // 将RDD[GenericRow]转为RDD[InternalRow]
      schema2) // 再讲RDD[GenericRow]加上一些表头信息变为LogicalRDD(output: Seq[Attribute], rdd: RDD[InternalRow])
    df2.show()

    // 一个DataFrame是由class DataFrame private[sql](override val sqlContext: SQLContext,val queryExecution: QueryExecution)实例化生成的。
    // 其中queryExecution起到了决定性作用。
    // sqlContext.executePlan会将LogicalPlan转为QueryExecution类型
    // 每个DataFrame在实例化的过程中会在内部生成一个logicalPlan变量，是经过sqlContext.analyzer.execute解析后的LogicalPlan，如果是command类或者insert或者create table类则直接优化语句并执行toRdd操作


    /** 看下DataFrame的执行 */
    // LogicalRDD是LogicalPlan的一种

    // spark中DataFrame进行transform操作时，是通过withPlan由一个LogicalPlan到另一个LogicalPlan的过程
    // 以select为例

    df2.select("id")
    //   def select(cols: Column*): DataFrame = withPlan {
    // Project(cols.map(_.named), logicalPlan)
    // 其中withPlan是由一个DataFrame到两个一个DataFrame的创建过程，
    // 即由一个LogicalPlan到另一个LogicalPlan的过程
    // Project是一个钟LogicalPlan，是UnaryNode的一种，即只涉及到一个DF的操作
    // 这里Project时一种新的LogicalPlan（project继承自UnaryNode（继承自LogicalPlan））。
    // LogicalPlan又继承自TreeNode，他具有child节点，project在创建的时候讲之前的LogicalPlan以child节点的形式保存。


    // 聚个更复杂的例子
    df2.join(df, Seq("id"), "inner").select("id", "person")


    // action操作

    df2.collect() //通过sqlContext优化后的方案，通过withCallback函数执行。


  }
}
