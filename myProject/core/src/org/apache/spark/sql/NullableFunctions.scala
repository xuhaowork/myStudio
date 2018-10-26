package org.apache.spark.sql

import org.apache.spark.sql.functions.{udf => normalUdf}
import scala.reflect.runtime.universe.TypeTag

/**
  * editor: xuhao
  * date: 2018.03.20 09:00:00
  */

/**
  * 用于处理sparkSQL中创建udf函数时null的输出问题，使udf函数可以实现null => null的映射
  * ----
  * 描述：
  * sparkSQL中的udf有以下弊端：
  * 输入可能带有null值，但输出不行，数据会发生信息损失。
  *    --原因在于Row中null不止代表String的缺失值还作为所有类型的缺失值，（因为column经常出现类型转换，e.g. string转long中
  *    null值就保留下来，此时long类型的列中就有null值），但一般的scala函数（包括udf）Int/Long/Double等数值型的缺失值却不是null
  *    ，编译不过。
  * e.g.
  * {{{
  * Long(with null) => Double中不能有null => null的映射：
  * udf((s: Long) => s match {
  *   case null => null
  *   case e => e / width
  * } // not compile
  * }}}
  * ----
  * NullableFunctions的udf将结果封进了Option类，可以实现null => null的映射，数据不会损失信息
  * e.g.
  * {{{
  * NullableFunctions.udf((s: Long) => s / width)
  * }}}
  * ----
  * The main source idea by Martin Senne's answer on Stack OverFlow.
  */
object NullableFunctions {
  def udf[RT: TypeTag, A1: TypeTag](f: A1 => RT)
  : UserDefinedFunction = normalUdf[Option[RT], A1] {
    case null => None
    case s => Some(f(s))
  }

  def udf[RT: TypeTag, A1: TypeTag, A2: TypeTag](f: (A1, A2) => RT)
  : UserDefinedFunction = normalUdf[Option[RT], A1, A2](
    (i1: A1, i2: A2) => (i1, i2) match {
      case (null, _) => None
      case (_, null) => None
      case (s1, s2) => Some(f(s1, s2))
    })

  def udfDealWithOption[RT: TypeTag, A1: TypeTag](f: A1 => Option[RT])
  : UserDefinedFunction = normalUdf[Option[RT], A1] {
    case null => None
    case s => f(s)
  }

}
