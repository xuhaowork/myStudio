package cn.datashoe.dataWrangling

import cn.datashoe.sparkUtils.DataSimulate
import org.scalatest.FunSuite

class PivotSuite extends FunSuite {
  test("pivot基本功能") {
//    val data = DataSimulate.salaryData(6000, 4)
//    data.show()
//
//    import org.apache.spark.sql.functions._
//    val result = data.groupBy("年龄").pivot("职业").agg(mean(col("薪资")))
//    result.show()
//
//    result.schema.apply("").dataType
//
//    import org.apache.spark.sql.types.{DataType, DoubleType, Decimal}
//    val da: DataType = null
//
//
//
//    println(DoubleType.typeName)
//    println(DoubleType.simpleString)

    import org.apache.spark.sql.functions._
    val column = col("薪资")



  }

  test("功能组织") {
    // 采用按键式的功能组织
    // ----
    // 行汇总 rowAgg
    // 列汇总 colAgg
    // 缺失值补全为0 naFill
    // 排序顺序 ascending
    // 列顺序 byValueSum byLabel
    // 限定列数 colsLimitNum
    // 限定列标签值 colsLabels


    // 列类型和值的要求:
    // 1)行标签对应列 | 可以多列    | 可以任意
    // 2)列标签对应列 | 单列        | 可以任意       | 缺失值会drop掉
    // 3)值对应列     | 单列        | 需要是数值类型  | 缺失值可以转为0


    // 输入
    // ----
    // 数据
    // 行标签对应列名 一个或多个
    // 列标签对应列名 一个
    // 值对应列名 一个

    import org.apache.spark.sql.ColumnUtils.ColumnImpl._

    val rowsLabelColName = Array("年龄", "职业")
    val colsLabelColName = "婚姻"
    val valueColName = "薪资"




    val data = DataSimulate.salaryData(6000, 5)
    data.show()

    import org.apache.spark.sql.functions._

    val uu = data.select("").groupBy("")
    val colLabelsSets = uu.agg(count(lit(1)).as("count")).orderBy(col("")).select("")

    colLabelsSets.cache()
    colLabelsSets.count()

    colLabelsSets.limit(2000)

    val colLabels = colLabelsSets.collect().map {
      row =>
        row.get(0).toString
    } // 还可以手动设定

    val dataSet = data.select(col("").cast("string"), col(""))

    dataSet.groupBy().pivot("").agg(col("")).na.fill(0.0)



  }



  test("pivot频次统计") {


  }


  test("列数超出") {


  }


  test("列汇总和行汇总") {

  }


}
