package com.self.core.VAR.VAR2.tests

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Matrix => BM, SparseVector => BSV}
import com.cloudera.sparkts._
import com.zzjz.deepinsight.basic.BaseMain
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, unix_timestamp}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer


/**
  * Created by dell on 2018/4/17.
  */
object Test extends BaseMain{
  override def run(): Unit = {
    /**
      * 设定时间序列的一些时间属性
      */
    val startTime = "2017-12-10 00:00:00"
    val endTime = "2017-12-10 00:10:00"
    import java.text.SimpleDateFormat

    val timeParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val startTimeStamp = timeParser.parse(startTime).getTime
    val endTimeStamp = timeParser.parse(endTime).getTime

    val frequencyNum = "1"
    val freqUnit = "m"

    val transForm = (unit: String, num: Int) => unit match {
      case "Y" => new YearFrequency(num)
      case "M" => new MonthFrequency(num)
      case "d" => new DayFrequency(num)
      case "h" => new HourFrequency(num)
      case "m" => new MinuteFrequency(num)
      case "s" => new SecondFrequency(num)
      case _ => throw new Exception("单位不是定长，以此单位生成时间序列需要一些特殊处理，请联系开发人员")
    }
    val frequency =  transForm(freqUnit, util.Try(frequencyNum.toInt) getOrElse 1)

    val timeIndex: UniformDateTimeIndex = sc.broadcast(
      DateTimeIndex.uniformFromInterval(startTimeStamp, endTimeStamp, frequency)).value

    import org.joda.time._
    new DateTime()

    val startDateTime = new DateTime(startTimeStamp)

    // month
//    val startFloor = startDateTime.monthOfYear().roundFloorCopy()
//    var tamp = startFloor.getMillis
//    val buff = ArrayBuffer(tamp)
//    var i = 1
//    while(tamp < endTimeStamp){
//      tamp = startFloor.plusMonths(i).monthOfYear().roundFloorCopy().getMillis
//      buff += tamp
//      i += 1
//    }
//
    // year
    val startFloor = startDateTime.yearOfCentury().roundFloorCopy()
    var tamp = startFloor.getMillis
    val buff = ArrayBuffer(tamp)
    var i = 1
    while(tamp < endTimeStamp){
      tamp = startFloor.plusMonths(i).monthOfYear().roundFloorCopy().getMillis
      buff += tamp
      i += 1
    }


    /**
      * 模拟数据
      */
    val arr = List(
      Array("1", "2017-12-10 00:00:00", 123, -150, -150),
      Array("1", "2017-12-10 00:01:00", 9, -150, -150),
      Array("1", "2017-12-10 00:03:00", 19, -10, -150),
      Array("1", "2017-12-10 00:01:01", 19, -150, -150),
      Array("1", "2017-12-10 00:02:00", 199, 50, -150),
      Array("1", "2017-12-10 00:04:00", 199, -150, -150),
      Array("1", "2017-12-10 00:09:00", 199, 0, 0),
      Array("2", "2017-12-10 00:07:00", 99, 0, -150),
      Array("2", null, 199, -150, -150),
      Array("2", "2017-12-10 00:01:00", 1, -50, -15),
      Array("2", "2017-12-10 00:02:02", 90, -0, -1),
      Array("2", "2017-12-10 00:02:02", 19, -50, -15))

    val rdd = sc.parallelize(arr).map(Row.fromSeq(_))
    var rawDataDF = sqlc.createDataFrame(rdd,
      StructType(Array(StructField("id", StringType),
        StructField("dayTime", StringType),
        StructField("x", IntegerType),
        StructField("y", IntegerType),
        StructField("z", IntegerType))))


    val col_id = "id"
    val arrayCol = Array(("x", "int"), ("y", "int"), ("z", "int"))


    val bufferCol = "dayTime"

    val fieldFormat = "StringType"

    val timeFormat = "yyyy-MM-dd HH:mm:ss"

    val col_type = rawDataDF.schema.fields.map(_.dataType)
      .apply(rawDataDF.schema.fieldIndex(bufferCol))
    rawDataDF = fieldFormat match {
      case "StringType" =>
        if(col_type != StringType){
          throw new Exception(s"$bufferCol 不是 StringType")
        }else{
          rawDataDF.withColumn(bufferCol + "_new", unix_timestamp(col(bufferCol), timeFormat).*(1000))
        }
      case "TimeStampType" =>
        if(col_type != TimestampType)
          throw new Exception(s"$bufferCol 不是 TimeStampType")
        rawDataDF.withColumn(bufferCol + "_new", col(bufferCol).cast(LongType).*(1000))
      case "UTC" => try{
        rawDataDF.withColumn(bufferCol + "_new", col(bufferCol).cast(LongType).*(1000))
      }catch{
        case _: Exception =>
          throw new Exception(s"$bufferCol 不是 LongType")
      }
    }

    rawDataDF.show()



    val value = sc.broadcast(buff.toArray).value
    val rawRdd = rawDataDF.rdd.map(r => {
      val id = r.getAs[String](col_id)
      val time = r.getAs[Long](bufferCol + "_new")
      val arr = arrayCol.map{
        case (name, colType) => colType match {
        case "int" => util.Try(r.getAs[Int](name).toDouble) getOrElse 0.0
        case "float" => util.Try(r.getAs[Float](name).toDouble) getOrElse 0.0
        case "double" => util.Try(r.getAs[Double](name)) getOrElse 0.0
        case "string" => util.Try(r.getAs[String](name).toDouble) getOrElse 0.0
        }
      }
	  val dt = new DateTime(time)
	  val roundTime = dt.yearOfCentury().roundFloorCopy().getMillis
	  val modTime = time - roundTime
      ((id, roundTime), (modTime, arr))
    })

    rawRdd.foreach(println)

//      .reduceByKey{
//      case ((modTime1, arr1), (modTime2, arr2)) =>
//        if(modTime1 <= modTime2) (modTime1, arr1) else (modTime2, arr2)
//    }.map{case ((id, roundTime), (_, v)) => (id, Map(roundTime -> v))}
//      .reduceByKey(_ ++ _).mapValues(indexMap => {
//      val arr = value.flatMap(time => indexMap.getOrElse(time, Array.fill(arrayCol.length)(Double.NaN)))
//      new BDM(value.length, arrayCol.length, arr, 0, 0, true)
//    })
//
//
//    rawRdd.foreach(println)



//
//
//
//
//
////    /**
////      * 时间上进行规整
////      */
////
////    rawRdd.flatMapValues{case (time, arr) => }
//
//

//    def matLagMat(mat: BM, maxLag: Int, includeOriginal: Boolean): Matrix = {
//      val colArrs = MatrixUtils.matToColArrs(mat)
//      var resultMatrix: Matrix = null
//      for (i <- 0 until mat.numCols) {
//        if (resultMatrix == null) {
//          resultMatrix = lagMat(new DenseVector(colArrs(i)),maxLag,includeOriginal)
//        } else {
//          resultMatrix = MatrixUtils.matConcat(resultMatrix, lagMat(new DenseVector(colArrs(i)),maxLag,includeOriginal))
//        }
//      }
//      resultMatrix
//    }













  }
}
