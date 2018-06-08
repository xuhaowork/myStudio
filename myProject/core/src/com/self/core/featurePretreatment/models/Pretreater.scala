package com.self.core.featurePretreatment.models

import org.apache.spark.sql.DataFrame

/**
  * editor: xuhao
  * date: 2018-06-08 08:30:00
  */

/**
  * 特征预处理的主类
  * ----
  * 约定特征预处理算子的规则:
  * 1)至少输入一个DataFrame
  * 2)至少输出一个DataFrame
  * 3)输入参数要合法
  * ----
  * 该脚本提供了：
  * 1)实现方法：设定参数和获取参数信息 @see [[setParams]] @see [[getParams]]
  * 2)实现方法：最后统一的输出 @see [[run()]]
  * 2)接口：运行函数的接口 @see [[runAlgorithm()]]
  * 3)接口：判定每个参数是否合法，@see [[setParams]]中的参数[checkParam]，该参数是一个函数
  * 4)接口：判定整个参数组是否满足应有的约束，@see [[paramsValid]]
  *
  * @param data 输入的主要变换的data(还可能有其他DataFrame输出)
  */
abstract class Pretreater[M <: PretreatmentOutput](val data: DataFrame) {

  /**
    * 参数组列表
    * 有一些非常常用的参数，约定一下参数名：
    * inputCol -> 输入列名
    * outputCol -> 输出列名
    */
  val params: java.util.HashMap[String, Any] = new java.util.HashMap[String, Any]()


  /**
    * 输入参数
    * ----
    *
    * @param name       参数名
    * @param param      参数
    * @return this
    */
  final def setParams(param: Any,
                      name: String): this.type = {
    this.params.put(name, param)
    this
  }

  /**
    * 输入参数
    * ----
    *
    * @param name       参数名
    * @param annotation 在算子中参数的识别信息 ——常见的如在平台上显示的中文参数名
    * @param param      参数
    * @param checkParam 每个参数合法的检查
    * @return this
    */
  final def setParams(
                       param: Any,
                       name: String,
                       annotation: String,
                       checkParam: Function3[Any, String, String, Boolean] = (_, _, _) => true): this.type = {
    require(checkParam(param, name, annotation), s"您输入的参数不合法：$annotation")
    this.params.put(name, param)
    this
  }

  /**
    * 得到输入参数
    * ----
    *
    * @param name       参数名
    * @param annotation 在算子中参数的识别信息 ——常见的如在平台上显示的中文参数名
    * @tparam T 参数的类型
    * @return
    */
  final def getParams[T](name: String, annotation: String): T = {
    lazy val exception = new IllegalArgumentException(s"您没有找到参数$annotation")
    val value = this.params.getOrDefault(name, exception)
    lazy val castTypeException = (msg: String) =>
      new ClassCastException(s"$annotation 不能转为指定类型, 具体信息: $msg")
    try {
      value.asInstanceOf[T]
    } catch {
      case e: Exception => throw castTypeException(e.getMessage)
    }
  }


  protected def paramsValid(): Boolean


  /**
    * run函数
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    * @see [[com.zzjz.deepinsight.core.featurePretreatment.models.PretreatmentOutput]]
    *      里面至少有一个输出的DataFrame，还可能有一个额外的输出信息。
    */
  final def run(): M = {
    require(data.schema.fieldNames.distinct.length < data.schema.size,
      "数据中有模糊列名会影响后续操作, 请检查是否有同名列") // 检查数据是否有模糊列名
    paramsValid() // 检查参数组信息是否合法
    runAlgorithm() // 执行运行函数
  }

  /**
    * 执行函数接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  protected def runAlgorithm(): M

}



