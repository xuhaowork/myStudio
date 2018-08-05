package com.self.core.generalTimeBinner.models

/**
  * PerformanceInfo系列
  * ----
  * 两个子类：
  * @define BinningTimePfm
  * @define BinningResultPfm 该子类包含参数:
  *                          byInterval --有三个选项"left", "right", "interval"代表展示分箱的左右端点还是展示分箱区间
  *                          resultType需要为[[ResultType]]类型 --用来存储分箱结果展示的样式信息
  */
trait PerformanceInfo extends Serializable

class BinningTimePfm extends PerformanceInfo
class BinningResultPfm(
                        val byInterval: String,
                        val resultType: ResultType
                      ) extends PerformanceInfo


/**
  * ResultType类型 --用于存储分箱结果的样式信息
  * ----
  * 包括三个子类：
  * @define StringResultType  包含参数timeFormatType --有三个选项："byHand", "select", "selfAdapt"
  *                                  "byHand"是需要输入handScript, "select"时需要输入select
  * @define LongResultType 需要输入参数unit --有两个选项"second", "millisecond"
  * @define TimestampResultType
  *
  */
trait ResultType extends Serializable

class StringResultType(
                        val timeFormatType: String,
                        val handScript: Option[String],
                        val select: Option[String]) extends ResultType

class LongResultType(val unit: String) extends ResultType

class TimestampResultType extends ResultType





