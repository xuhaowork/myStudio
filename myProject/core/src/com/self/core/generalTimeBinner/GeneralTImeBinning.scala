package com.self.core.generalTimeBinner

import java.sql.Timestamp
import java.util.TimeZone

import com.google.gson.{Gson, JsonParser}
import com.zzjz.deepinsight.basic.BaseMain
import com.zzjz.deepinsight.core.featurePretreatment.utils.Tools.{columnExists, columnTypesIn}
import com.zzjz.deepinsight.core.generalTimeBinner.models._
import com.zzjz.deepinsight.core.generalTimeBinner.tools.{BinningUDFUtil, Utils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, NullableFunctions, Row, UserDefinedFunction}
import org.joda.time._


object GeneralTImeBinning extends BaseMain {
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
    //    /** 通过单条数据测试 */
    //        testSingle() // 测试一下

    /** 通过单条数据测试分箱时间是否能够以Array形式展示 */
    //    testUDT()

    /** 0)获得一些系统变量还有初始数据 */
    val jsonparam = "<#zzjzParam#>"
    val gson = new Gson()
    val p: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])
    val parser = new JsonParser()
    val pJsonParser = parser.parse(jsonparam).getAsJsonObject
    val z1 = z
    val rddTableName = "<#zzjzRddName#>"

    val tableName = try {
      p.get("inputTableName").trim
    } catch {
      case e: Exception => throw new Exception(s"没有找到您输入的预测表参数，具体信息${e.getMessage}")
    }

    val rawDataFrame = try {
      z1.rdd(tableName).asInstanceOf[org.apache.spark.sql.DataFrame]
    } catch {
      case e: Exception => throw new Exception(s"获取预测表${tableName}过程中失败，具体信息${e.getMessage}")
    }

    val sQLContext = rawDataFrame.sqlContext
    val sparkContext = sQLContext.sparkContext

    /** 1)解析器 */
    /** 需要时间列名、列类型、时间信息 --窄口进 */
    val timeColName = pJsonParser
      .get("timeColName")
      .getAsJsonArray
      .get(0)
      .getAsJsonObject
      .get("name")
      .getAsString

    columnExists(timeColName, rawDataFrame, true) // 确认列名存在
    columnTypesIn(timeColName, rawDataFrame, true,
      StringType, LongType, TimestampType, new BinningTimeUDT) // 列类型需要为其中之一

    val timeColType: DataType = rawDataFrame.schema.apply(timeColName).dataType

    val timeColInfoObj = pJsonParser
      .get("timeColInfoObj").getAsJsonObject

    val format = timeColInfoObj.get("value").getAsString
    val timeColInfo = timeColType match {
      case StringType =>
        require(format == "string", s"您输入的时间列类型为$format，而实际类型为string，" +
          s"如果确定为${format}而只是类型不一致，您可以通过数据建模算子转换")
        val timeFormatObj = timeColInfoObj.get("timeFormatObj").getAsJsonObject
        val timeFormat = timeFormatObj.get("timeFormatType")
          .getAsJsonObject.get("timeFormat").getAsString
        new StringTypeTimeColInfo(timeColName, timeFormat)

      case LongType =>
        require(format == "long", s"您输入的时间列类型为$format，而实际类型为long，" +
          s"请在时间字段类型选择长整型时间戳")
        val unit = timeColInfoObj.get("unit").getAsString
        new LongTypeTimeColInfo(timeColName, unit)

      case TimestampType =>
        require(format == "timestamp", s"您输入的时间列类型为$format，而实际类型为timestamp，" +
          s"请在时间字段类型选择时间戳类型")
        new TimestampTypeTimeColInfo(timeColName)

      case _: BinningTimeUDT =>
        require(format == "binningTime", s"您输入的时间列类型为$format，而实际类型为binningTime，" +
          s"请在时间字段类型选择分箱时间类型")
        val forLeft = timeColInfoObj.get("forLeft").getAsString == "true"
        new BinningTypeTimeColInfo(timeColName, forLeft)
    }
    val timeParser = new TimeParser(timeColInfo)

    /** 2)相位设置并结合分箱信息构造分箱信息 */
    // 按时长分箱
    /** 需要相位的绝对相对信息、相位的时长信息 */

    val phaseObj = pJsonParser.get("phase").getAsJsonObject
    val phaseType = phaseObj.get("value").getAsString //"relative" // "absolute"
    val phase = phaseType match {
      case "absolute" =>
        val timeString = phaseObj.get("timeString").getAsString
        new DateTime(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeString).getTime)
      case "relative" =>

        /** 相对时间需要输入时间字符串和选择时间字符串的格式 */
        val timeString = phaseObj.get("timeString").getAsString
        val format = phaseObj.get("format").getAsString
        val sdf = new java.text.SimpleDateFormat(format)
        sdf.setTimeZone(TimeZone.getTimeZone("UTC")) // 相对信息需要设置为UTC时间
        new DateTime(sdf.parse(timeString).getTime).withZone(DateTimeZone.UTC)
    }
    val window = try {
      pJsonParser.get("window").getAsString.toLong
    } catch {
      case e: Exception => throw new Exception(s"获取分箱窗宽信息时出现错误，具体信息${e.getMessage}")
    }


    val unit = pJsonParser.get("unit").getAsString

    val binningInfoValue = timeColInfo match {
      case info: BinningTypeTimeColInfo =>
        new TimeBinnerInfo(phase, window.toLong, unit, info.forLeft)

      case _: TimeColInfo =>
        new TimeBinnerInfo(phase, window.toLong, unit, true)
    }

    val binningInfo = sparkContext.broadcast(binningInfoValue).value

    /** 3)获得分箱展示信息，并构造分箱的UDF */
    val outputCol = pJsonParser.get("outputCol").getAsString

    val performanceTypeObj = pJsonParser.get("performanceTypeObj").getAsJsonObject
    val performanceInfo = try{
      BinningUDFUtil.readFromJson(performanceTypeObj)
    }catch {
      case e: Exception => throw new Exception(s"将展示模式解析为能够序列化的类型时失败, ${e.getMessage}")
    } // 将JsonObject存到能够序列化的类中

    val binningUDF = BinningUDFUtil.constructUDF(timeParser, binningInfo, performanceInfo)

    val newDataFrame = rawDataFrame.withColumn(outputCol, binningUDF(col(timeColName)))


  }
}
