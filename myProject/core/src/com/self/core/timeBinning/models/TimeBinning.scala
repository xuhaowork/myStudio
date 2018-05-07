package com.self.core.timeBinning.models

import com.zzjz.deepinsight.core.utils.TimeColInfo
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_unixtime, unix_timestamp}
import org.apache.spark.sql.types.{LongType, StructType, TimestampType}

/**
  *
  * @param timeColInfo
  * @param binningInfo
  * 所有数据转换同一精确到秒
  */
class TimeBinning(val timeColInfo: TimeColInfo,
                  val binningInfo: BinningInfo,
                  val presentInfo: PresentInfo) extends Serializable {
  var newTimeCol: TimeColInfo = new TimeColInfo("timeCol", "long", Some("second"))
  var binningTimeCol = new TimeColInfo("newCol", "long", Some("second"))
  var binningTimeColBackUp: Option[TimeColInfo] = None
  var binningTimeColExtra: Option[TimeColInfo] = None


  /**
    * 时间列转换
    *
    * @param data 输入的数据
    * @return 转换后的数据，增加了一列以秒为单位的UTC时间
    */
  def transform(data: DataFrame): DataFrame = {
    timeColInfo.check(data.schema) // 检查时间列名
    newTimeCol = newTimeCol.checkName(data.schema) // 检查新建的时间列名是否存在于

    this.timeColInfo.dataType match {
      case "string" =>
        val timeFormat = try {
          this.timeColInfo.timeFormat.get
        } catch {
          case _: Exception => throw new Exception("error105: 当时间列类型为string时需要有timeFormat")
        }
        val newName = newTimeCol.name
        data.withColumn(newName, unix_timestamp(col(timeColInfo.name), timeFormat))

      case "long" =>
        val utcFormat = timeColInfo.timeFormat.get
        utcFormat match {
          case "second" =>
            newTimeCol.update(timeColInfo.name, "long") // 此时不用转换，原有的就是恰好的
            data.withColumn(newTimeCol.name, col(timeColInfo.name).cast(LongType))
          case "millisecond" =>
            data.withColumn(newTimeCol.name, col(timeColInfo.name).cast(LongType)./(1000L))
        }
        newTimeCol.update(timeColInfo.name, "long") // 此时不用转换，原有的就是恰好的
        data.withColumn(newTimeCol.name, col(timeColInfo.name).cast(LongType))
    }
  }


  /**
    * 分箱的主要算法
    * ----
    * 将transform输出的以秒为单位的UTC时间数据进行分箱操作，输出分箱开始时间对应的UTC（以秒为单位）
    *
    * @param data 输入数据
    * @return
    */
  def binning(data: DataFrame): DataFrame = {
    val transformDF = transform(data: DataFrame) // 将时间转换为秒的形式
    binningTimeCol = binningTimeCol.checkName(transformDF.schema) // 检查新建的时间列名是否存在于

    binningInfo match {
      case info: FixedLengthInfo =>
        val (width, startTimeStamp) = try {
          (info.length, info.startTimeStamp)
        } catch {
          case _: Exception => throw new Exception("error201,参数信息异常: " +
            "没有找到等宽分箱的宽度或起始相位信息")
        }
        try {
          val binningTime = Tools.binningByFixedLength(width, startTimeStamp)
          transformDF.withColumn(binningTimeCol.name, binningTime(col(newTimeCol.name)))
        } catch {
          case failure: Exception => throw new Exception(s"error501，数据处理异常：分箱时出现异常。$failure")
        }

      case info: NaturalOneStepInfo =>
        val slidingUnit: String = try {
          info.slidingUnit
        } catch {
          case _: Exception => throw new Exception("error201,参数信息异常: " +
            "没有找到精确平滑的单位信息")
        }
        try {
          val binningTime = Tools.slidingByNatural(slidingUnit) // second
          transformDF.withColumn(binningTimeCol.name, binningTime(col(newTimeCol.name)))
        } catch {
          case failure: Exception => throw new Exception(s"error501，数据处理异常：分箱时出现异常。$failure")
        }
      case info: NaturalTwoStepInfo =>
        val (slidingUnit1: String, slidingUnit2: String) = try {
          (info.slidingUnit1, info.slidingUnit2)
        } catch {
          case _: Exception => throw new Exception("error201,参数信息异常: " +
            "没有找到精确平滑的单位信息")
        }
        try {
          val binningTime = Tools.slidingByNatural(slidingUnit1) // second
          binningTimeColExtra = Some(new TimeColInfo(binningTimeCol.name + "_extra", "long", Some("second"))
            .checkName(transformDF.schema)) // 剩余的时间
          var newDF = transformDF.withColumn(binningTimeCol.name, binningTime(col(newTimeCol.name)))
          val binningTime2 = Tools.slidingByNatural(slidingUnit2) // second
          newDF = newDF.withColumn(binningTimeColExtra.get.name,
            binningTime2(col(newTimeCol.name) - col(binningTimeCol.name)))
          newDF
        } catch {
          case failure: Exception => throw new Exception(s"error501，数据处理异常：分箱时出现异常。$failure")
        }
    }
  }


    /**
      * 将精确到秒的UTC时间数据转化为对应的形式 --俗称，打扮一下更好看
      * ----
      * @param data 输入数据，里面有分箱结果（每个分箱的开始时间 --精确到秒的UTC时间）
      */
    def dress(data: DataFrame) = {
      presentInfo.format match {
        case "selfAdaption" => {



        }
        case "design" =>
          val presentColInfo = try {
            presentInfo.timeColInfo.get
          } catch {
            case _: Exception => throw new Exception("error201,参数信息异常: " +
              "没有找到要展示的时间列的信息")
          }
          if(data.schema.fieldNames contains presentColInfo.name)
            throw new Exception("error201, 参数信息异常: "
              + "您输入的展示时间列名已经存在，请更换列名")
          presentColInfo.dataType match {
            case "string" => data.withColumn(presentColInfo.name,
              from_unixtime(col(binningTimeCol.name), presentColInfo.timeFormat.get))
            case "long" => presentColInfo.timeFormat.get match {
              case "second" => data.withColumnRenamed(binningTimeCol.name, presentColInfo.name)
              case "millisecond" => data.withColumn(presentColInfo.name, col(binningTimeCol.name).*(1000L))
            }
            case "timestamp" => data.withColumn(presentColInfo.name,
              col(binningTimeCol.name).*(1000).cast(TimestampType))
          }

      }


    }


  /**
    * 根据初始的时间列信息和分箱信息按一定规则决定以何种形式输出
    * 规则如下：
    * 1）如果时自然时间二级分箱则统一按字符串输出，且输出两列，一列为一级分箱的时间，另一列为二级分箱的时间
    * 2）
    * @param data
    */
  private def selfAdaption(data: DataFrame) = {
    binningInfo match {
      case info: NaturalTwoStepInfo =>
        val (slidingUnit1, slidingUnit2) = (info.slidingUnit1, info.slidingUnit2)




    }

  }

  private def unitToTimeFormat(unit: String) = unit match {
    case "year" => "yyyy"
    case "season" => "yyyy-MM"
    case "month" => "yyyy-MM"
    case "week" => "yyyy-MM-ddEEE"
    case "day" => "yyyy-MM-dd"
    case "hour" => "yyyy-MM-dd HH"
    case "minute" => "yyyy-MM-dd HH:mm"
    case "second" => "yyyy-MM-dd HH:mm:ss"
  }
  "yyyy-MM-dd HH:mm:ss"
  "0123456789012345678"
  private def unitId(unit: String) = {
    unit match {
      case "year" => (0, 4)
      case "month" => (5, 7)
      case "season" => (5, 7)
      case "week" => (8, 10)
      case "day" => (8, 10)
      case "hour" => (11, 13)
      case "minute" => (14, 16)
      case "second" => (17, 19)
    }

  }

  private def unitToTimeFormatTail(unit1: String, unit2: String) = {
    "yyyy-MM-dd HH:mm:ss"

  }




}
