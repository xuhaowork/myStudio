package com.self.core.generalTimeBinner.tests

import java.sql.Timestamp
import java.util.TimeZone

import com.self.core.baseApp.myAPP
import com.self.core.generalTimeBinner.models._
import com.self.core.generalTimeBinner.tools.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, NullableFunctions, Row, UserDefinedFunction}
import org.joda.time._


object GeneralTImeBinning extends myAPP {
  val testData1 = Array(
    ("2017/02/10 00:00:00", 1, 2),
    ("2017/02/08 01:00:01", 1, 2),
    ("2017/3/1 04:00:02", 1, 2),
    ("2017/4/10 00:15:03", 1, 2),
    (null, 1, 2),
    ("2017/04/20 07:20:05", 1, 2),
    ("2017/04/30 08:01:06", 1, 2),
    ("2017/04/30 09:11:06", 1, 2),
    ("2017/04/30 16:01:06", 1, 2),
    ("2017/06/10 13:01:06", 1, 2),
    ("2017/08/10 00:00:00", 1, 2),
    ("2017/08/18 01:00:01", 1, 2),
    ("2017/11/1 04:00:02", 1, 2),
    ("2017/12/31 00:15:03", 1, 2),
    ("2017/04/10 06:20:04", 1, 2),
    ("2018/01/1 07:20:05", 1, 2),
    ("2018/02/19 13:01:06", 1, 2),
    ("2018/03/2 13:01:06", 1, 2),
    ("2018/03/9 13:01:06", 1, 2),
    ("2018/04/1 13:01:06", 1, 2))

  def testSingle(): Unit = {
    /** 构造数据 */
    val timeColName = "time"
    val timeFormat = "yyyy/MM/dd HH:mm:ss"
    val time1 = "2018/01/8 13:01:06" // string格式时间
    val time2 = new java.text.SimpleDateFormat(timeFormat).parse(time1).getTime // long类型数据
    val time3 = new Timestamp(time2) // timestamp类型数据
    println("构造数据：")
    println(s"string格式时间:$time1")
    println(s"long格式时间:$time2")
    println(s"timestamp格式时间:$time3")
    println()

    /** 解析器 */
    val timeColInfo = new StringTypeTimeColInfo(timeColName, "yyyy/MM/dd HH:mm:ss")
    val binningTime1 = new TimeParser(timeColInfo).parse(time1)

    val timeColInfo2 = new LongTypeTimeColInfo(timeColName, "millisecond")
    val binningTime2 = new TimeParser(timeColInfo2).parse(time2)

    val timeColInfo3 = new TimestampTypeTimeColInfo(timeColName)
    val binningTime3 = new TimeParser(timeColInfo3).parse(time3)

    println("测试解析器：")
    println(s"string格式时间:$binningTime1")
    println(s"long格式时间:$binningTime2")
    println(s"timestamp格式时间:$binningTime3")
    println()

    /** 分箱器 */
    val phase: DateTime = {
      val time1 = "2018/01/1 0:0:0"
      new DateTime(new java.text.SimpleDateFormat(timeFormat).parse(time1).getTime).withZone(DateTimeZone.getDefault)
    }

    val window = 2 // 小时
    val binningInfo = new TimeBinnerInfo(phase, window.toLong, "month", true)
    binningTime1.binning(binningInfo)

    println("测试分箱器：")
    println(s"一级分箱:${binningTime1.getBinningResultToString(Some("yyyy-MM-ddEEE HH:mm:ss"), "left")}")

    val phase2: DateTime = {
      val time1 = "01-01 0:0:0"

      val sdf = new java.text.SimpleDateFormat("MM-dd HH:mm:ss")
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
      new DateTime(sdf.parse(time1).getTime).withZone(DateTimeZone.UTC)
    }
    println("phase2", phase2)

    val binningInfo2Grade = new TimeBinnerInfo(phase2, 10, "day", false) // 对剩余时间继续分箱

    binningTime1.binning(binningInfo2Grade)

    println(s"二级分箱:${binningTime1.getBinningResultToString(None, "left")}")


    println()


    val phase3: DateTime = {
      val time1 = "0:0:0"
      val sdf = new java.text.SimpleDateFormat("HH:mm:ss")
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
      new DateTime(sdf.parse(time1).getTime).withZone(DateTimeZone.UTC)
    }
    val binningInfo3Grade = new TimeBinnerInfo(phase3, 12, "hour", false) // 对剩余时间继续分箱

    binningTime1.binning(binningInfo3Grade) // 继续按第三级分箱，对上一级分箱的分箱结果进行分箱
    println(s"三级分箱:${binningTime1}")

  }


  def testUDT() = {
    /** 构造数据 */
    val timeColName = "time"
    val timeFormat = "yyyy/MM/dd HH:mm:ss"
    val time1 = "2018/01/8 13:01:06" // string格式时间
    val time2 = new java.text.SimpleDateFormat(timeFormat).parse(time1).getTime // long类型数据
    val time3 = new Timestamp(time2) // timestamp类型数据

    /** 解析器 */
    val timeColInfo = new StringTypeTimeColInfo(timeColName, "yyyy/MM/dd HH:mm:ss")
    val binningTime1 = new TimeParser(timeColInfo).parse(time1)

    val timeColInfo2 = new LongTypeTimeColInfo(timeColName, "millisecond")
    val binningTime2 = new TimeParser(timeColInfo2).parse(time2)

    val timeColInfo3 = new TimestampTypeTimeColInfo(timeColName)
    val binningTime3 = new TimeParser(timeColInfo3).parse(time3)


    /** 分箱器 */
    val phase: DateTime = {
      val time1 = "2018/01/1 0:0:0"
      new DateTime(new java.text.SimpleDateFormat(timeFormat).parse(time1).getTime).withZone(DateTimeZone.getDefault)
    }

    val window = 2 // 小时
    val binningInfo = new TimeBinnerInfo(phase, window.toLong, "month", true)
    binningTime1.binning(binningInfo)

    println("测试分箱器：")
    println(s"一级分箱:${binningTime1.getBinningResultToString(Some("yyyy-MM-ddEEE HH:mm:ss"), "left")}")

    println()
    println("binningTime展示为数组形式")
    println(binningTime1.castAllInfoToArray.mkString(","))

    println()

    val phase2: DateTime = {
      val time1 = "01-01 0:0:0"

      val sdf = new java.text.SimpleDateFormat("MM-dd HH:mm:ss")
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
      new DateTime(sdf.parse(time1).getTime).withZone(DateTimeZone.UTC)
    }

    val binningInfo2Grade = new TimeBinnerInfo(phase2, 10, "day", false) // 对剩余时间继续分箱

    binningTime1.binning(binningInfo2Grade)

    println(s"二级分箱:${binningTime1.getBinningResultToString(None, "left")}")


    println()
    println("binningTime展示为数组形式")
    println(binningTime1.castAllInfoToArray.mkString(","))

    println()

    println("binningTime展示为数组形式")

    val result = binningTime1.castAllInfoToArray
    println(result.mkString(","))

    val reflect = Utils.reConstruct(result)
    println("还原:", reflect)
    println("是否相等", reflect.castAllInfoToArray.mkString(",") == result.mkString(","))


    // 测试UDT是否可行

    //    var element: TimeMeta, // 不变
    //    var leftChild: Option[BinningTime], // 会变
    //    var rightChild: Option[BinningTime],
    //    var isLeaf: Boolean,
    //    var deep: Int,
    //    var position: Int = 0,
    //    var parentPosition: Int = 0
    val testData3 = Array(
      //      (
      //       new BinningTime(
      //        new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss")),
      //        new BinningTime(new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"),
      //          None,
      //          None,
      //          true,
      //          1,
      //          -1,
      //          0),
      //        new BinningTime(new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"),
      //          None,
      //          None,
      //          true,
      //          1,
      //          1,
      //          0),
      //        false,
      //        0,
      //        0,
      //        0
      //      )
      Tuple1(binningTime1)
    )

    val rdd3 = sc.parallelize(testData3).map(Row.fromTuple)


    /** 先通过rdd看看到底核心的数据流转需要什么类型工程结构，形成较为清晰的需求 */

    val phase3: DateTime = {
      val time1 = "0:0:0"
      val sdf = new java.text.SimpleDateFormat("HH:mm:ss")
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
      new DateTime(sdf.parse(time1).getTime).withZone(DateTimeZone.UTC)
    }
    val binningInfo3Grade = new TimeBinnerInfo(phase3, 12, "hour", false) // 对剩余时间继续分箱

    binningTime1.binning(binningInfo3Grade) // 继续按第三级分箱，对上一级分箱的分箱结果进行分箱
    println(s"三级分箱:${binningTime1}")


    val rawDataFrame3 = sqlc.createDataFrame(rdd3, StructType(Array(
      StructField("time", new BinningTimeUDT),
      StructField("col1", IntegerType),
      StructField("col2", IntegerType))))

    rawDataFrame3.show()


    val udf4binningTime = NullableFunctions.udf(
      (bt: BinningTime) => {
        bt.binning(binningInfo3Grade)
        bt.getBinningResultToString(None, "interval")
      }
    )

    rawDataFrame3.select(udf4binningTime(col("time")).as("binningTime")).show()


    val df = sqlc.createDataFrame(Seq(((1, 2.0), (2.0, 3)), ((1, 2.0), (2.0, 3)))).toDF("first", "second")

    df.show()
    df.schema.foreach(println)


  }

  val rawDataFrame2: DataFrame =
    sqlc.createDataFrame(testData1).toDF("time", "id", "value")


  override def run(): Unit = {
//    //    /** 通过单条数据测试 */
//    //        testSingle() // 测试一下
//
//    /** 通过单条数据测试分箱时间是否能够以Array形式展示 */
//        testUDT()
//
//
//    /** 0)获得一些系统变量还有初始数据 */
//
//
//    val rawDataFrame = rawDataFrame2
//
//
////    val numFeatures = try {
////      z1.rdd(p.get("numFeatures")).asInstanceOf[Option[Int]].get
////    } catch {
////      case e: Exception => throw new Exception(s"获得训练特征数信息时出现异常，具体信息${e.getMessage}")
////    }
////
////
////    val featureColsArr = pJsonParser.get("featureCols").getAsJsonArray
//
//
//    val sQLContext = rawDataFrame.sqlContext
//    val sparkContext = sQLContext.sparkContext
//
//    /** 1)解析器 */
//    /** 需要时间列名、列类型、时间信息 --窄口进 */
//    val timeColName = "time"
//    import com.self.core.featurePretreatment.utils.Tools.{columnExists, columnTypesIn}
//
//    columnExists(timeColName, rawDataFrame, true) // 确认列名存在
//    columnTypesIn(timeColName, rawDataFrame, true,
//      StringType, LongType, TimestampType, new BinningTimeUDT) // 列类型需要为其中之一
//
//    val timeColType: DataType = rawDataFrame.schema.apply(timeColName).dataType
//
//    var binningForResult = true
//    val timeColInfo = timeColType match {
//      case StringType =>
//        val timeFormat = "yyyy/MM/dd HH:mm:ss"
//        new StringTypeTimeColInfo(timeColName, timeFormat)
//
//      case LongType =>
//        val unit = "second"
//        new LongTypeTimeColInfo(timeColName, unit)
//
//      case TimestampType =>
//        new TimestampTypeTimeColInfo(timeColName)
//
//      case _: BinningTimeUDT =>
//        binningForResult  = "" == "true"
//        new BinningTypeTimeColInfo(timeColName, true)
//    }
//    val timeParser = new TimeParser(timeColInfo)
//
//    /** 2)相位设置并结合分箱信息构造分箱信息 */
//    // 按时长分箱
//    /** 需要相位的绝对相对信息、相位的时长信息 */
//    val phaseType = "relative" // "absolute"
//    val phase = phaseType match {
//      case "absolute" =>
//        val timeString = "2018-01-01 0:0:0"
//        new DateTime(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeString).getTime)
//      case "relative" =>
//
//        /** 相对时间需要输入时间字符串和选择时间字符串的格式 */
//        val timeString = "00:00:00" // 需要用SimpleDateFormat解析
//      val format = "HH:mm:ss"
//        val sdf = new java.text.SimpleDateFormat(format)
//        sdf.setTimeZone(TimeZone.getTimeZone("UTC")) // 相对信息需要设置为UTC时间
//        new DateTime(sdf.parse(timeString).getTime).withZone(DateTimeZone.UTC)
//    }
//    val window = 2
//    val unit = "hour"
//    val binningInfo = sparkContext.broadcast(new TimeBinnerInfo(phase, window.toLong, unit, true)).value
//
//    /** 3)获得分箱展示信息，并构造分箱的UDF */
//    val outputCol = "output"
//    val performanceType = "onlyBinningResult" // "binningTime", "onlyBinningResult"
//
//    val binningUDF: UserDefinedFunction = performanceType match {
//      case "binningTime" =>
//        NullableFunctions.udf(
//          (any: Any) => {
//            /** 解析为BinningTime */
//            val parserTime: BinningTime = timeParser.parse(any)
//            parserTime.binning(binningInfo)
//          })
//
//      case "onlyBinningResult" =>
//        // "interval", "left", "right"
//        val byInterval = "left"
//        // "string" // "long" // "timestamp"
//        val resultType = "timestamp"
//        resultType match {
//          // string是udf始终是BinningTime => string
//          case "string" =>
//            // "byHand" // "select" // "selfAdapt"
//            val timeFormatType = "byHand"
//            val timeFormat = timeFormatType match {
//              case "byHand" =>
//                val handScript = "yyyy-MM-ddEEE HH:mm:ss"
//                Some(handScript)
//              case "select" =>
//                val select = "yyyy-MM-dd HH:mm:ss"
//                Some(select)
//              case "selfAdapt" =>
//                None
//            }
//
//            NullableFunctions.udf(
//              (any: Any) => {
//                /** 解析为BinningTime */
//                val parserTime: BinningTime = timeParser.parse(any)
//                parserTime.binning(binningInfo)
//                parserTime.getBinningResultToString(timeFormat, byInterval)
//              })
//
//
//          case "long" => // long以区间展示是string类型，其他为long类型
//            val unit = "second"
//            if (byInterval == "interval") {
//              NullableFunctions.udf(
//                (any: Any) => {
//                  /** 解析为BinningTime */
//                  val parserTime: BinningTime = timeParser.parse(any)
//                  parserTime.binning(binningInfo)
//                  val left =
//                    if (unit == "second")
//                      parserTime.getBinningBySide("left").getMillis / 1000
//                    else
//                      parserTime.getBinningBySide("left").getMillis
//                  val right =
//                    if (unit == "second")
//                      parserTime.getBinningBySide("left").getMillis / 1000
//                    else
//                      parserTime.getBinningBySide("left").getMillis
//                  "[" + left + ", " + right + "]"
//                })
//            } else {
//              NullableFunctions.udf(
//                (any: Any) => {
//                  /** 解析为BinningTime */
//                  val parserTime: BinningTime = timeParser.parse(any)
//                  parserTime.binning(binningInfo)
//
//                  if (unit == "second")
//                    parserTime.getBinningBySide(byInterval).getMillis / 1000
//                  else
//                    parserTime.getBinningBySide(byInterval).getMillis
//                })
//            }
//
//          case "timestamp" =>
//            NullableFunctions.udf(
//              (any: Any) => {
//                /** 解析为BinningTime */
//                val parserTime: BinningTime = timeParser.parse(any)
//                parserTime.binning(binningInfo)
//                val millis = parserTime.getBinningBySide(byInterval).getMillis
//                new Timestamp(millis)
//              })
//        }
//
//    }
//
//    rawDataFrame.withColumn(outputCol, binningUDF(col(timeColName))).show()








    val arg1 =
      """
        |import com.google.gson.{Gson, JsonParser}
        |// import com.zzjz.deepinsight.basic.BaseMain
        |import org.apache.spark.sql.{DataFrame, Row}
        |import com.zzjz.deepinsight.core.taxi.urbanGroupAnalysis.models.IdMapUtil
        |import org.apache.spark.sql.types.{StringType, StructField, StructType,IntegerType}
        |
        |    val jsonparam = "<#jsonparam#>"
        |    val gson = new Gson()
        |    val pGson: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])
        |
        |    //解析嵌套形式的json
        |    val parser = new JsonParser()
        |    val pJsonParser = parser.parse(jsonparam).getAsJsonObject()
        |
        |    val intervalTypeOb = pJsonParser.getAsJsonObject("intervalType")
        |    val intervalType = intervalTypeOb.get("value").getAsString
        |
        |    val tableName = pGson.get("tableName")
        |    val longitudeName = pGson.get("longitudeName")
        |    val latitudeName = pGson.get("latitudeName")
        |    val startLongitude = pGson.get("startLongitude").toDouble
        |    val endLongitude = pGson.get("endLongitude").toDouble
        |    val startLatitude = pGson.get("startLatitude").toDouble
        |    val endLatitude = pGson.get("endLatitude").toDouble
        |
        |    var intervalLongitude:Double = 0.0
        |    var intervalLatitude:Double = 0.0
        |
        |    if(intervalType == "km"){
        |      val intervalKm = intervalTypeOb.get("intervalKm").getAsDouble
        |      //注意，这里纬度永远是度数和距离是固定的比例，但是经度随着纬度的不同，度数和距离的大小是变化的，这里姑且让经度和纬度的度数间隔保持一致
        |      intervalLatitude = intervalKm/100.0
        |      intervalLongitude = intervalLatitude
        |    }else{
        |      intervalLongitude = intervalTypeOb.get("intervalLongitude").getAsDouble
        |      intervalLatitude = intervalTypeOb.get("intervalLatitude").getAsDouble
        |    }
        |
        |    val z1 = z
        |
        |    if ( z1.rdd(tableName) == null ){
        |      throw new Exception("输入的表不存在，请重新输入")
        |    }
        |
        |    if ( startLongitude >= endLongitude || startLatitude >= endLatitude ){
        |      throw new Exception("经纬度终止值小于起始值，请重新输入")
        |    }
        |
        |    val inputDF = z1.rdd(tableName).asInstanceOf[DataFrame].na.drop(Array(longitudeName, latitudeName))
        |
        |    //得到经纬度细分
        |    val LongitudeList = IdMapUtil.getList(startLongitude,endLongitude,intervalLongitude)
        |    val LatitudeList = IdMapUtil.getList(startLatitude,endLatitude,intervalLatitude)
        |    val LoList = sc.broadcast(LongitudeList)
        |    val LaList = sc.broadcast(LatitudeList)
        |
        |    try{
        |      inputDF.schema.fieldIndex(longitudeName)
        |      inputDF.schema.fieldIndex(latitudeName)
        |    }
        |    catch {
        |      case e:Exception => throw new Exception("经度或者纬度列名不存在，请重新输入")
        |    }
        |
        |    val rdd = inputDF.map(r => {
        |      val lo = r(r.fieldIndex(longitudeName)).toString.toDouble
        |      val la = r(r.fieldIndex(latitudeName)).toString.toDouble
        |      val loNum = IdMapUtil.findLocation(lo,LoList.value)
        |      val laNum = IdMapUtil.findLocation(la,LaList.value)
        |      val id = (loNum-1)*(LaList.value.length-1)+laNum
        |      val s = id +: r.toSeq
        |      Row.fromSeq(s)
        |    })
        |
        |    val schema = StructField("", IntegerType, true) +: inputDF.schema
        |
        |    val outDF = sqlc.createDataFrame(rdd, StructType(schema))
        |
        |    outDF.cache()
        |    outputrdd.put("<#rddtablename#>", outDF)
        |    outDF.registerTempTable("<#rddtablename#>")
        |    sqlc.cacheTable("<#rddtablename#>")
      """.stripMargin



    val arg2 =
      """
        |import com.google.gson.{Gson, JsonParser}
        |// import com.zzjz.deepinsight.basic.BaseMain
        |import org.apache.spark.sql.{DataFrame, Row}
        |import com.zzjz.deepinsight.core.taxi.urbanGroupAnalysis.models.IdMapUtil
        |import org.apache.spark.sql.types.{StringType, StructField, StructType,IntegerType}
        |
        |    val jsonparam = "<#jsonparam#>"
        |    val gson = new Gson()
        |    val pGson: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])
        |
        |    //解析嵌套形式的json
        |    val parser = new JsonParser()
        |    val pJsonParser = parser.parse(jsonparam).getAsJsonObject()
        |
        |    val intervalTypeOb = pJsonParser.getAsJsonObject("intervalType")
        |    val intervalType = intervalTypeOb.get("value").getAsString
        |
        |    val tableName = pGson.get("tableName")
        |    val longitudeName = pGson.get("longitudeName")
        |    val latitudeName = pGson.get("latitudeName")
        |    val startLongitude = pGson.get("startLongitude").toDouble
        |    val endLongitude = pGson.get("endLongitude").toDouble
        |    val startLatitude = pGson.get("startLatitude").toDouble
        |    val endLatitude = pGson.get("endLatitude").toDouble
        |
        |    var intervalLongitude:Double = 0.0
        |    var intervalLatitude:Double = 0.0
        |
        |    if(intervalType == "km"){
        |      val intervalKm = intervalTypeOb.get("intervalKm").getAsDouble
        |      //注意，这里纬度永远是度数和距离是固定的比例，但是经度随着纬度的不同，度数和距离的大小是变化的，这里姑且让经度和纬度的度数间隔保持一致
        |      intervalLatitude = intervalKm/100.0
        |      intervalLongitude = intervalLatitude
        |    }else{
        |      intervalLongitude = intervalTypeOb.get("intervalLongitude").getAsDouble
        |      intervalLatitude = intervalTypeOb.get("intervalLatitude").getAsDouble
        |    }
        |
        |    val z1 = z
        |
        |    if ( z1.rdd(tableName) == null ){
        |      throw new Exception("输入的表不存在，请重新输入")
        |    }
        |
        |    if ( startLongitude >= endLongitude || startLatitude >= endLatitude ){
        |      throw new Exception("经纬度终止值小于起始值，请重新输入")
        |    }
        |
        |    val inputDF = z1.rdd(tableName).asInstanceOf[DataFrame].na.drop(Array(longitudeName, latitudeName))
        |
        |    //得到经纬度细分
        |    val LongitudeList = IdMapUtil.getList(startLongitude,endLongitude,intervalLongitude)
        |    val LatitudeList = IdMapUtil.getList(startLatitude,endLatitude,intervalLatitude)
        |    val LoList = sc.broadcast(LongitudeList)
        |    val LaList = sc.broadcast(LatitudeList)
        |
        |    try{
        |      inputDF.schema.fieldIndex(longitudeName)
        |      inputDF.schema.fieldIndex(latitudeName)
        |    }
        |    catch {
        |      case e:Exception => throw new Exception("经度或者纬度列名不存在，请重新输入")
        |    }
        |
        |    val rdd = inputDF.map(r => {
        |      val lo = r(r.fieldIndex(longitudeName)).toString.toDouble
        |      val la = r(r.fieldIndex(latitudeName)).toString.toDouble
        |      val loNum = IdMapUtil.findLocation(lo,LoList.value)
        |      val laNum = IdMapUtil.findLocation(la,LaList.value)
        |      val cenLo = (LoList.value(loNum)+LoList.value(loNum-1))/2
        |      val cenLa = (LaList.value(laNum)+LaList.value(laNum-1))/2
        |      val s = (cenLo.formatted("%.5f")+","+cenLa.formatted("%.5f")) +: r.toSeq
        |      Row.fromSeq(s)
        |    })
        |
        |    val schema = StructField("", StringType, true) +: inputDF.schema
        |
        |    val outDF = sqlc.createDataFrame(rdd, StructType(schema))
        |
        |    outDF.cache()
        |    outputrdd.put("<#rddtablename#>", outDF)
        |    outDF.registerTempTable("<#rddtablename#>")
        |    sqlc.cacheTable("<#rddtablename#>")
      """.stripMargin

    println(arg1 == arg2)

    val maxLength = scala.math.max(arg1.length, arg2.length)
    var string1 = ""
    var string2 = ""
    var flag = true
    for (i <- 0 until maxLength) {
      if(flag && util.Try(arg1.charAt(i).toString).getOrElse("") == util.Try(arg2.charAt(i).toString).getOrElse("")){

        }else {
          flag = false
          string1 += util.Try(arg1.charAt(i).toString).getOrElse("")
          string2 += util.Try(arg2.charAt(i).toString).getOrElse("")
        }
    }
    println("string1", string1)
    println("string2", string2)

















  }
}
