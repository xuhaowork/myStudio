package cn.datashoe.dataWrangling

import cn.datashoe.sparkBase.SparkAPP
import cn.datashoe.sparkUtils.DataSimulate


object Pivot extends SparkAPP {
  override def run(): Unit = {
    //
    val data = DataSimulate.salaryData(6000, 5, Some(1123L))
    import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

    val conf = sc.hadoopConfiguration
    val hdfs: FileSystem = FileSystem.get(conf)

    val savePath = "G:\\aa"
    val path = new Path(savePath)

    if (hdfs.exists(path)) {
      if (hdfs.isDirectory(path))
        hdfs.delete(path, true)
      else hdfs.delete(path, false)
    }
    if (hdfs.exists(path)) {
      if (hdfs.isDirectory(path))
        hdfs.delete(path, true)
      else hdfs.delete(path, false)
    }

    val split = ","
    val header = "true"
    val newPath = new Path("G:\\sss.csv")
//    org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
    data.repartition(1).write.format("com.databricks.spark.csv")
      .option("header", header)
      .option("delimiter", split)
      .save(path.toString)
    FileUtil.copyMerge(path.getFileSystem(conf), path, newPath.getFileSystem(conf), newPath, false, conf, null)


//    import org.apache.poi.xssf.usermodel.{XSSFFormulaEvaluator, XSSFRow, XSSFSheet, XSSFWorkbook}


  }
}



import com.google.gson.JsonParser
import com.zzjz.deepinsight.basic.BaseMain
import com.zzjz.deepinsight.core.utils.pretreatmentUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

object Pivot2 extends SparkAPP {
  override def run(): Unit = {
    // 采用按键式的功能组织
    // ----
    // 行汇总 rowAgg
    // 列汇总 colAgg
    // 缺失值补全为0 naFill
    // 排序顺序 ascending
    // 列顺序 orderMode : byValueSum byLabel
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

    /**
      * 1.参数处理
      */
    val jsonParam = "<#jsonparam#>"
    val parserJson = new JsonParser().parse(jsonParam).getAsJsonObject

    /** 1)表格 */
    val tableName = parserJson.get("inputTableName").getAsString
    val data = z.rdd(tableName).asInstanceOf[org.apache.spark.sql.DataFrame]
    try {
      data.schema.length
    } catch {
      case _: NullPointerException => throw new Exception(s"未能成功获取${tableName}对应的表")
    }

    /** 2)基本参数设置 */
    // 行标签所在列名
    val rowsLabelColName = Array.range(0, parserJson.get("rowsLabelColName").getAsJsonArray.size()).map {
      i =>
        val name = parserJson.get("rowsLabelColName").getAsJsonArray.get(i).getAsJsonObject.get("name").getAsString
        pretreatmentUtils.columnExists(name, data, requireExist = true)
        name
    }
    // 列标签所在列名 --单列 要求是原子类型
    val colsLabelColName = {
      val name = parserJson.get("colsLabelColName").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
      pretreatmentUtils.columnExists(name, data, requireExist = true)
      name
    }
    // 值所在列名 --单列 要求是数值类型
    val valueColName = {
      val name = parserJson.get("valueColName").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
      pretreatmentUtils.columnExists(name, data, requireExist = true)
      name
    }
    // 值聚合方式 --单选 `mean` `avg`, `max`, `min`, `sum`, `count`
    val aggFun = parserJson.get("aggFun").getAsString

    /** 3)高级设置 */
    val advancedSettings = parserJson.get("advancedSettings").getAsJsonObject
    // 是否进行高级设置 --单选 `是` `否`
    val advanced = advancedSettings.get("value").getAsString

    val (orderMode, ascendingInCols, ascendingInRows, preColsLabels, colsLimitNum,
    rowAgg, colAgg, ratio, byPercent, percentScale, naFill) = advanced match {
      case "true" =>
        // 列标签排序 --单选  `byValueSum` `byLabel`
        val orderMode = advancedSettings.get("orderMode").getAsString
        // 列标签排序模式 --单选 `ascending` `descending`
        val ascendingInCols = advancedSettings.get("ascendingInCols").getAsString == "ascending"
        // 行标签排序模式 --单选 `ascending` `descending`
        val ascendingInRows = advancedSettings.get("ascendingInRows").getAsString == "ascending"
        // 预设列标签值 --输入框: 以英文逗号分隔 默认为空 空格无效
        val preColsLabelsString = advancedSettings.get("preColsLabels").getAsString
        val preColsLabels: Option[Array[String]] = if (preColsLabelsString == null || preColsLabelsString.nonEmpty) {
          Some(preColsLabelsString.split(",").map(_.trim))
        } else {
          None
        }
        // 最大列数 --输入框 最大不能超过10000
        val colsLimitNumString = advancedSettings.get("colsLimitNumString").getAsString
        val colsLimitNum: Option[Int] = if (colsLimitNumString == null || colsLimitNumString.isEmpty) {
          val num = pretreatmentUtils.cast2Numeric(colsLimitNumString)
          require(num > 1, s"最大列数${num}应该超过1")
          require(num <= 10000, s"最大列数${num}不能超过10000")
          require(num.toInt == num, s"最大列数${num}需要为整数")
          Some(num.toInt)
        } else
          None

        // 行总计
        val rowAgg = advancedSettings.get("rowAgg").getAsString == "true"
        // 列总计
        val colAgg = advancedSettings.get("colAgg").getAsString == "true"
        // 占比统计 --`否` `统计行占比` `统计列占比` --"false" "byRowRatio" "byColRatio"
        val ratioObj = advancedSettings.get("ratio").getAsJsonObject
        val ratio = ratioObj.get("value").getAsString
        //     嵌套: 以百分比形式显示占比 --`是` `否 `  百分比小数位数2
        val (byPercent, percentScale)= ratio match {
          case "false" => (false, "2")
          case _ => (ratioObj.get("byPercent").getAsString == "true", ratioObj.get("percentScale").getAsString)
        }
        // 将缺失值填充为0
        val naFill = advancedSettings.get("naFill").getAsString == "true"

        (orderMode, ascendingInCols, ascendingInRows,
          preColsLabels, colsLimitNum, rowAgg, colAgg,
          ratio, byPercent, percentScale, naFill)

      case "false" =>
        // 默认参数
        ("byLabel", true, true, None, None,
          false, false, "false", false, "2", true)
    }

    import org.apache.spark.sql.functions.monotonically_increasing_id


    /**
      * 2.数据处理
      */
    /** 变换前数据处理 */
    import org.apache.spark.sql.functions._
    // 筛选列名
    val rawData = data.select(colsLabelColName, valueColName +: rowsLabelColName: _*).filter(col(colsLabelColName).isNotNull)
    // 列排序依据列名
    val sortedCol = if (orderMode == "byValueSum") col("count") else col(colsLabelColName)

    def contains(colsLabels: Array[String]) = udf {
      (value: Any) =>
        colsLabels.contains(value.toString)
    }

    require(rawData.count() > 0, "输入数据为空不能进行计算")

    // 获得列标签 --此处没有直接将preColsLabels作为colLabels, 而是进行了一次统计, 目的是为了照顾到排序功能
    val colLabelsSets = if (preColsLabels.isDefined) {
      val aa = rawData.groupBy(colsLabelColName)
        .agg(count(lit(1)).as("count"))
        .orderBy(if (ascendingInCols) sortedCol else sortedCol.desc)

      require(aa.count() > 0,
        s"输入数据不能为空, 且列标签'$colsLabelColName'和分组列${rowsLabelColName.mkString("'", "', '", "'")}" +
          s"不同时为空的记录数不能小于等于0")

      val bb = aa.filter(contains(preColsLabels.get)(col(colsLabelColName)))
      bb.cache()
      require(bb.count() > 0, "发现按照您设定的参数列标签为空, 请您查看数据列标签是否全为空或者是否您设定的列标签值不在数据中" +
        "(提示: 注意列标签值所在列的数据类型是否是String或者数值)")
      bb
    } else {
      val aa = rawData
        .groupBy(colsLabelColName)
        .agg(count(lit(1)).as("count"))
        .orderBy(if (ascendingInCols) sortedCol else sortedCol.desc)

      aa.cache()
      require(aa.count() > 0,
        s"输入数据不能为空, 且列标签'$colsLabelColName'和分组列${rowsLabelColName.mkString("'", "', '", "'")}不同时为空的记录数不能小于等于0")
      aa
    }

    // 提取前若干个
    val colLabels = if (colsLimitNum.isEmpty)
      try {
        colLabelsSets.collect().map {
          row =>
            row.get(0).toString
        }
      } catch {
        case e: Exception => throw new Exception(s"在获取列标签时失败, 具体信息: ${e.getMessage}")
      }
    else
      try {
        colLabelsSets.take(colsLimitNum.get).map {
          row =>
            row.get(0).toString
        }
      } catch {
        case e: Exception => throw new Exception(s"在获取列标签时失败, 具体信息: ${e.getMessage}")
      }

    require(colLabels.nonEmpty,
      "发现按照您设定的参数列标签为空, 请您查看数据列标签是否全为空或者是否您设定的列标签值不在数据中" +
        "(提示: 注意列标签值所在列的数据类型是否是String或者数值)")

    /** 一些列名和列类型设置 */
    // 一些临时的列名
    object tmpColName extends Serializable {
      val colsLabelColName2string: String = colsLabelColName + "_str"
      val valueColName2double: String = valueColName + "_dou"
      val rowAggColName = "行汇总"
      val colAggColName = "列汇总"
    }
    val valueType = rawData.schema(valueColName).dataType
    val rowLabelColsType = rowsLabelColName.map { name => name -> data.schema(name).dataType }.toMap

    // 将值类型转为double @note 注意decimal此时精度会降低
    val dataSet = rawData
      .select(
        col(colsLabelColName).cast(StringType).as(tmpColName.colsLabelColName2string) +:
          col(valueColName).cast(DoubleType).as(tmpColName.valueColName2double) +:
          rowsLabelColName.map(col): _*
      )


    val pivotRes: DataFrame = if (naFill)
      dataSet.groupBy(rowsLabelColName.map(col): _*)
        .pivot(tmpColName.colsLabelColName2string, colLabels)
        .agg(Map(tmpColName.valueColName2double -> aggFun))
        .na.fill(0.0)
        .orderBy(rowsLabelColName.map(name => if (ascendingInRows) col(name) else col(name).desc): _*)
    else
      dataSet.groupBy(rowsLabelColName.map(col): _*)
        .pivot(tmpColName.colsLabelColName2string, colLabels)
        .agg(Map(tmpColName.valueColName2double -> aggFun))
        .orderBy(rowsLabelColName.map(name => if (ascendingInRows) col(name) else col(name).desc): _*)

    def getResult(colAgg: Boolean, rowAgg: Boolean): DataFrame = {
      // todo: 上千列的时候reduce是否合适? 可以来个性能对比: reduce[Column] ?? | rdd.map{sum} ??
      val selectCols =
        if (rowAgg)
          rowsLabelColName.map(col) ++ colLabels.map { name => col(name).cast(valueType) } :+
            colLabels.map { name => col(name) }.reduceLeft[Column] { case (res, cl) => res + cl }
              .as(tmpColName.rowAggColName)
        else
          rowsLabelColName.map(col) ++ colLabels.map { name => col(name).cast(valueType) }

      val rowAggRes = if (colAgg) {
        val aggResDF = pivotRes.select(
          colLabels.map(name => sum(col(name)).as(name)): _*)
        val aggRes: Row = aggResDF.collect().head
        val aggResSchema = aggResDF.schema.fields
        val uu = pivotRes.sqlContext.createDataFrame(
          pivotRes.sqlContext.sparkContext.parallelize(
            Array(Row.merge(Row.fromSeq(rowsLabelColName.map(_ => null)), aggRes))
          ),
          StructType(
            rowsLabelColName.map { name => StructField(name, StringType) } ++ aggResSchema
          )
        ).select(rowsLabelColName.map { name => col(name).cast(rowLabelColsType(name)) } ++ colLabels.map(col): _*)
        pivotRes.union(uu)
      }
      else
        pivotRes

      rowAggRes.select(selectCols: _*)
    }


    val newDataFrame = ratio match {
      case "false" =>
        getResult(colAgg, rowAgg)

      // 行占比
      case "byRowRatio" =>
        // 此时自动按行累计求和
        val result = getResult(colAgg: Boolean, true)
        val rowRdd = result.rdd.map {
          row =>
            // 分组列
            val groups = rowsLabelColName.map {name => row.getAs[Any](name)}
            val cols = colLabels.map { name => if(row.isNullAt(row.fieldIndex(name))) Double.NaN else 0.0 }
            val sumByRow = row.getAs[Double](tmpColName.rowAggColName)
            val colsWithSum = cols :+ sumByRow
            val valueRatios = if(!byPercent) {
              colsWithSum.map {
                element => if(sumByRow == 0.0) null else (element / sumByRow).toString
              }
            } else {
              val scale = pretreatmentUtils.cast2Numeric(percentScale)
              require(scale == scale.toInt && scale > 0, s"需要小数点位数'$scale'为正整数")
              val stringFormat = s"%.${scale.toInt}f%%"
              cols.map {
                element =>
                  if(sumByRow == 0.0)
                    null
                  else
                    stringFormat.format(element * 100 / sumByRow)
              }
            }

            Row.merge(Row.fromSeq(groups), Row.fromSeq(valueRatios))
        }

        val pivotSchema = colLabels :+ tmpColName.rowAggColName

        val foo = pivotRes.sqlContext.createDataFrame(
          rowRdd, StructType(
            StructType(
              rowsLabelColName.map {
                name => StructField(name, StringType)
              } ++ pivotSchema.map { name => StructField(name, StringType)}
            )
          )
        )

        if(!byPercent)
          foo
        else
          foo.select(rowsLabelColName.map(col) ++ pivotSchema.map { name => col(name).cast(valueType)}:_*)

      // 列占比
      case "byColRatio" =>
        // 此时自动按列求和
        val result = getResult(true, rowAgg)



    }


    def ratio2(aggRes: Row, pivotRes: DataFrame, ratio: String, byPercent: Boolean) = {





    }


    //    res.show(20000)


  }
}

