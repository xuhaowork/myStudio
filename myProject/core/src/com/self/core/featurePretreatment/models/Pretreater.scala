package com.self.core.featurePretreatment.models


import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag


/**
  * editor: xuhao
  * date: 2018-06-08 08:30:00
  */

/**
  * 特征预处理的抽象类
  * ----
  * 脚本设计思路：
  * 1）实现了实际中预处理的算法；
  * 2）这些算法的[输入数据]和[输出数据]很相似（都至少一个DataFrame）；
  * 3）这些算法的[运行函数]和[参数]差异很大。
  * 因此 ->
  * 1）将[数据]、[输出函数run()]写在抽象类中；
  * 2）将[参数]、[运行函数]作为接口；
  * 3）为了预备将来可能加入新的方法，这里将输出类型做成了一个泛型（虽然目前只有一种输出———DataFrame）。
  * ----
  * 具体来讲，该脚本提供了：
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


  /** 参数信息以[[ParamInfo]]的形式传入 */
  final def setParams(paramInfo: ParamInfo, param: Any): this.type = {
    this.params.put(paramInfo.name, param)
    this
  }

  /** 参数信息以[[ParamInfo]]的形式传入 */
  final def setParams(paramInfo: ParamInfo,
                      param: Any,
                      checkParam: Function3[String, Any, String, Boolean] = (_, _, _) => true): this.type = {
    require(checkParam(paramInfo.name, param, paramInfo.annotation), s"您输入的参数不合法：${paramInfo.annotation}")
    this.params.put(paramInfo.name, param)
    this
  }


  /** 传入的参数信息为[[ParamInfo]]的形式 */
  final def getParams[T <: Any : ClassTag](paramInfo: ParamInfo): T = {

    lazy val exception = new IllegalArgumentException(s"您没有找到参数${paramInfo.annotation}")
    val value = this.params.getOrDefault(paramInfo.name, exception)
    lazy val castTypeException = (msg: String) =>
      new ClassCastException(s"${paramInfo.annotation} 不能转为指定类型${}, 具体信息: $msg")
    try {
      value.asInstanceOf[T]
    } catch {
      case e: Exception => throw castTypeException(e.getMessage)
    }
  }


  final def getParamsOrElse[T <: Any : ClassTag](paramInfo: ParamInfo, other: T): T = {
    val value = this.params.getOrDefault(paramInfo.name, other).asInstanceOf[T]
    lazy val castTypeException = (msg: String) =>
      new ClassCastException(s"${paramInfo.annotation} 不能转为指定类型${}, 具体信息: $msg")
    try {
      value.asInstanceOf[T]
    } catch {
      case e: Exception => throw castTypeException(e.getMessage)
    }
  }


  protected def paramsValid(): Boolean


  /**
    * [输出函数run()]
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    * @see [[com.self.core.featurePretreatment.models.PretreatmentOutput]]
    *      里面至少有一个输出的DataFrame，还可能有一个额外的输出信息。
    */
  final def run(): M = {
    require(data.schema.fieldNames.distinct.length == data.schema.size,
      "数据中有模糊列名会影响后续操作, 请检查是否有同名列") // 检查数据是否有模糊列名
    paramsValid() // 检查参数组信息是否合法
    runAlgorithm() // 执行运行函数
  }

  /**
    * [运行函数]接口
    *
    * @return 输出一个PretreatmentOutput的实现子类型
    */
  protected def runAlgorithm(): M

}

/**
  * 正则表达式分词
  */
class TokenizerByRegex(override val data: DataFrame) extends Pretreater[OnlyOneDFOutput](data) {
  override protected def paramsValid(): Boolean = true

  override protected def runAlgorithm(): OnlyOneDFOutput = {
    import org.apache.spark.ml.feature.RegexTokenizer

    val inputCol = getParams[String](TokenizerByRegexParamsName.inputCol)
    val outputCol = getParams[String](TokenizerByRegexParamsName.outputCol)
    val gaps = getParamsOrElse[Boolean](TokenizerByRegexParamsName.gaps, true)
    val toLowerCase = getParamsOrElse[Boolean](TokenizerByRegexParamsName.toLowerCase, false)
    val pattern = getParamsOrElse[String](TokenizerByRegexParamsName.pattern, "\\s+")
    val minTokenLength = getParamsOrElse[Int](TokenizerByRegexParamsName.minTokenLength, 0)

    val tokenizer = new RegexTokenizer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setGaps(gaps)
      .setToLowercase(toLowerCase)
      .setPattern(pattern)
      .setMinTokenLength(minTokenLength)
    val wordsData = try {
      tokenizer.transform(this.data)
    } catch {
      case e: Exception => throw new Exception("正则表达式分词转换过程中出现异常，" +
        s"请检查是否是正则表达式或者一些参数发生了错误，${e.getMessage}")
    }
    new OnlyOneDFOutput(wordsData)
  }
}


class CountWordVector(override val data: DataFrame) extends Pretreater[OnlyOneDFOutput](data) {
  /** 2-2 向量计数器 */
  override protected def paramsValid(): Boolean = true

  override protected def runAlgorithm(): OnlyOneDFOutput = {

    import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}

    val inputCol = getParams[String](CountWordVectorParamsName.inputCol)
    val outputCol = getParams[String](CountWordVectorParamsName.outputCol)
    val loadModel = getParamsOrElse[Boolean](CountWordVectorParamsName.loadModel, false)

    val cvModel = if (loadModel) {
      val loadPath = getParams[String](CountWordVectorParamsName.loadPath)
      CountVectorizerModel.load(loadPath)
    } else {
      val trainData = getParamsOrElse[DataFrame](CountWordVectorParamsName.trainData, data) // 如果不输入默认训练数据就是预测数据

      val trainInputCol = getParamsOrElse[String](CountWordVectorParamsName.trainInputCol,
        CountWordVectorParamsName.inputCol.name) // 如果不输入默认训练数据就是预测数据

      val trainOutputCol = getParamsOrElse[String](CountWordVectorParamsName.trainOutputCol,
        CountWordVectorParamsName.outputCol.name) // 如果不输入默认训练数据就是预测数据

      val vocabSize = getParamsOrElse[Int](CountWordVectorParamsName.vocabSize, 1 << 18) // 默认 2^18
      val minDf = getParamsOrElse[Double](CountWordVectorParamsName.minDf, 1.0) // 默认1.0
      val minTf = getParamsOrElse[Double](CountWordVectorParamsName.minTf, 1.0) // 默认1.0 这里会设置一次，因为可能持久化

      val model = new CountVectorizer()
        .setInputCol(trainInputCol)
        .setOutputCol(trainOutputCol)
        .setVocabSize(vocabSize)
        .setMinDF(minDf)
        .setMinTF(minTf)
        .fit(trainData)

      val saveModel = getParamsOrElse[Boolean](CountWordVectorParamsName.saveModel, false) // 默认不保存
      val savePath = getParams[String](CountWordVectorParamsName.savePath) // if 保存 --必须输入
      if (saveModel)
        model.save(savePath)

      model
    }

    val minTf = getParams[Double](CountWordVectorParamsName.minTf)
    val newDataFrame = cvModel
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setMinTF(minTf)
      .transform(data)
    new OnlyOneDFOutput(newDataFrame)
  }

}


case class ParamInfo(name: String,
                     annotation: String) // 参数判断写在类外面了

class ParamsName {
  /** 输入列名 */
  val inputCol = ParamInfo("inputCol", "预处理列名")

  /** 输出列名 */
  val outputCol = ParamInfo("outputCol", "输出列名")
}


object TokenizerByRegexParamsName extends ParamsName {
  /** 是以此为分隔还是以此为匹配类型 */
  val gaps = ParamInfo("gaps", "是以此为分隔还是以此为匹配类型")

  /** 匹配的模式 */
  val pattern = ParamInfo("pattern", "匹配的模式")

  /** 是否将英文转为小写 */
  val toLowerCase = ParamInfo("toLowerCase", "是否将英文转为小写")

  /** 最小分词长度 --不满足的将会被过滤掉 */
  val minTokenLength = ParamInfo("minTokenLength", "最小分词长度")
}

object CountWordVectorParamsName extends ParamsName {
  val loadModel = ParamInfo("loadModel", "是否从持久化引擎中获取词频模型") // 是/否
  /** if [[loadModel]] == 是：需要进一步的训练信息如下 */
  val loadPath = ParamInfo("loadPath", "模型读取路径") // end if

  /** if [[loadModel]] == 否：需要进一步的训练信息如下 */
  val trainData = ParamInfo("trainData", "训练数据")
  val trainInputCol = ParamInfo("trainInputCol", "词汇数")
  val trainOutputCol = ParamInfo("trainOutputCol", "词汇数")
  val vocabSize = ParamInfo("VocabSize", "词汇数")
  val minDf = ParamInfo("minDf", "最小文档频率")
  val minTf = ParamInfo("minTf", "最小词频")
  /** 是否保存新训练的模型 */
  val saveModel = ParamInfo("saveModel", "是否将模型保存到持久化引擎中") // 是/否
  /** if[[saveModel]] == 是 */
  val savePath = ParamInfo("savePath", "模型保存路径") // end if // end if


}




