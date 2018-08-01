package com.self.core.generalTimeBinner

import java.sql.Timestamp
import java.util.TimeZone

import com.self.core.baseApp.myAPP
import com.self.core.generalTimeBinner.models._
import com.self.core.generalTimeBinner.tools.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{NullableFunctions, Row}
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
    println(s"一级分箱:${binningTime1.getBinningResult(Some("yyyy-MM-ddEEE HH:mm:ss"), "left")}")

    val phase2: DateTime = {
      val time1 = "01-01 0:0:0"

      val sdf = new java.text.SimpleDateFormat("MM-dd HH:mm:ss")
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
      new DateTime(sdf.parse(time1).getTime).withZone(DateTimeZone.UTC)
    }
    println("phase2", phase2)

    val binningInfo2Grade = new TimeBinnerInfo(phase2, 10, "day", false) // 对剩余时间继续分箱

    binningTime1.binning(binningInfo2Grade)

    println(s"二级分箱:${binningTime1.getBinningResult(None, "left")}")


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
    println(s"一级分箱:${binningTime1.getBinningResult(Some("yyyy-MM-ddEEE HH:mm:ss"), "left")}")

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

    println(s"二级分箱:${binningTime1.getBinningResult(None, "left")}")


    println()
    println("binningTime展示为数组形式")
    println(binningTime1.castAllInfoToArray.mkString(","))

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
    val rawDataFrame3 = sqlc.createDataFrame(rdd3, StructType(Array(
      StructField("time", new BinningTimeUDT),
      StructField("col1", IntegerType),
      StructField("col2", IntegerType))))

    rawDataFrame3.show()


    val udf4binningTime = NullableFunctions.udf(
      (bt: BinningTime) => {
        bt.getTipNode(true)
        bt
      }
    )

    rawDataFrame3.select(udf4binningTime(col("time")).as("binningTime")).show()


    val df = sqlc.createDataFrame(Seq(((1, 2.0), (2.0, 3)), ((1, 2.0), (2.0, 3)))).toDF("first", "second")

    df.show()
    df.schema.foreach(println)


  }


  override def run(): Unit = {
    //    /** 通过单条数据测试 */
    //        testSingle() // 测试一下


    /** 通过单条数据测试分箱时间是否能够以Array形式展示 */
    testUDT()


    //    val testData1 = Array(
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (null, 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2),
    //      (new RelativeMeta(100000L, Some(1000000L), "HH:mm:ss"), 1, 2))
    //
    //
    //    val testData2 = Array(
    //      (new DenseVector(Array(1.0)), 1, 2),
    //      (new DenseVector(Array(2.0)), 1, 2),
    //      (new DenseVector(Array(3.0)), 1, 2))
    //
    //
    //    val rdd = sc.parallelize(testData1).map(Row.fromTuple)
    //    val rdd2 = sc.parallelize(testData2).map(Row.fromTuple)
    //
    //
    //    /** 先通过rdd看看到底核心的数据流转需要什么类型工程结构，形成较为清晰的需求 */
    //    val rawDataFrame = sqlc.createDataFrame(rdd, StructType(Array(
    //      StructField("time", new TimeMetaUDT),
    //      StructField("col1", IntegerType),
    //      StructField("col2", IntegerType))))
    //
    //    rawDataFrame.show()
    //
    //
    //    val rawDataFrame2 = sqlc.createDataFrame(rdd2, StructType(Array(
    //      StructField("time", new VectorUDT),
    //      StructField("col1", IntegerType),
    //      StructField("col2", IntegerType))))
    //
    //
    //    val ud2 = NullableFunctions.udf((mt: DenseVector) => new DenseVector(Array(0.0)))
    //
    //    import org.apache.spark.sql.functions.col
    //
    //    rawDataFrame2.select(ud2(col("time"))).show()
    //
    //
    //    val ud3 = NullableFunctions.udf((mt: TimeMeta) => mt.createNew(0, Some(10 * 60 * 60 * 1000)))
    //    //    val ud = UserDefinedFunction((mt: TimeMeta) => mt.createNew(0, Some(10 * 60 * 60 * 1000)),
    //    //      new TimeMetaUDT, new TimeMetaUDT :: Nil)
    //
    //    rawDataFrame.select(ud3(col("time")).as("uuu")).show()
    //
    //






    //    /** 1)解析器 */
    //    /** 需要时间列名、列类型、时间信息 --窄口进 */
    //    val timeColName = "time"
    //    val timeColType = StringType // 这里要精准匹配
    //    val timeFormat = "yyyy/MM/dd HH:mm:ss"
    //    val timeColInfo = new StringTypeTimeColInfo(timeColName, timeFormat)
    //    val timeParser = new TimeParser(timeColInfo)
    //
    //    /** 2)相位设置 */
    //    // 按时长分箱
    //    // 分箱时长 单位为毫秒
    //    val phaseType = "relative" // "absolute"
    //    val phase = phaseType match {
    //      case "absolute" =>
    //        val timeString = "2018/01/1 0:0:0"
    //        new DateTime(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeString).getTime)
    //      case "relative" =>
    //        val timeString = "00:00:00" // 需要用SimpleDateFormat解析
    //      val format = "HH:mm:ss"
    //        val sdf = new java.text.SimpleDateFormat(format)
    //        sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    //        new DateTime(sdf.parse(timeString).getTime).withZone(DateTimeZone.UTC)
    //    }
    //    val window = 2
    //    val binningInfo = new TimeBinnerInfo(phase, window.toLong, "hour", true)


  }
}
