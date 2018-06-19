package com.self.core.featurePretreatment.objects

import com.google.gson.{Gson, JsonParser}
import com.zzjz.deepinsight.basic.BaseMain
import com.zzjz.deepinsight.core.featurePretreatment.models._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.DataFrame


object FeaturePretreatment extends BaseMain {
  val data: DataFrame = {
    sqlc.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes, which i like mostly"),
      (1, "Does Logistic regression models has a implicit params, Halt?")
    )).toDF("label", "sentence")
  }
  val rddTableName = "<#zzjzRddName#>"

  data.cache()
  outputrdd.put(rddTableName, data)
  data.registerTempTable(rddTableName)
  data.sqlContext.cacheTable(rddTableName)

  override def run(): Unit = {
    /**
      * 一些参数的处理
      */
    /** 0)获取基本的系统变量 */
    val jsonparam = "<#zzjzParam#>"
    val gson = new Gson()
    val p: java.util.Map[String, String] = gson.fromJson(jsonparam, classOf[java.util.Map[String, String]])
    val z1 = z


    /** 1)获取DataFrame */
    val tableName = p.get("inputTableName").trim
    val rawDataFrame = z1.rdd(tableName).asInstanceOf[org.apache.spark.sql.DataFrame]
    val parser = new JsonParser()
    val pJsonParser = parser.parse(jsonparam).getAsJsonObject

    val pretreatmentObj = pJsonParser.getAsJsonObject("pretreatmentType")
    val pretreatmentType = pretreatmentObj.get("value").getAsString
    val newDataFrame = pretreatmentType match {
      case "attributesDataTransform" => // 属性类特征转换  一个输入一个输出
        val inputCol = pretreatmentObj.get("inputCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
        val outputCol = pretreatmentObj.get("outputCol").getAsString

        val pretreatObj = pretreatmentObj.get("pretreatment").getAsJsonObject
        pretreatObj.get("value").getAsString match {
          case "TokenizerByRegex" =>
            val pattern = pretreatObj.get("pattern").getAsString

            val gaps = try {
              pretreatObj.get("gaps").getAsString == "true"
            } catch {
              case _: Exception => throw new Exception("是否转为小写，参数输入出现异常")
            }

            val minTokenLength = try {
              pretreatObj.get("minTokenLength").getAsString.toDouble.toInt
            } catch {
              case _: Exception => throw new Exception("您输入的最小分词数不能转为Int类型")
            }

            new TokenizerByRegex(rawDataFrame)
              .setParams(TokenizerByRegexParamsName.inputCol, inputCol)
              .setParams(TokenizerByRegexParamsName.outputCol, outputCol)
              .setParams(TokenizerByRegexParamsName.gaps, gaps)
              .setParams(TokenizerByRegexParamsName.minTokenLength, minTokenLength)
              .setParams(TokenizerByRegexParamsName.pattern, pattern)
              .run()
              .data

          case "CountWordVector" =>
            val vocabSizeString = try {
              pretreatObj.get("vocabSize").getAsString.trim
            } catch {
              case e: Exception => throw new Exception(s"没有找到词汇数参数有误，具体错误为：${e.getMessage}")
            }

            val vocabSize = if (vocabSizeString contains '^') {
              try {
                vocabSizeString.split('^').map(_.trim.toDouble).reduceLeft((d, s) => scala.math.pow(d, s)).toInt
              } catch {
                case e: Exception => throw new Exception(s"您输入词汇数参数表达式中包含指数运算符^，" +
                  s"但^两侧可能包含不能转为数值类型的字符，或您输入的指数过大超过了2^31，具体错误为.${e.getMessage}")
              }
            } else {
              try {
                vocabSizeString.toDouble.toInt
              } catch {
                case e: Exception => throw new Exception(s"输入的词汇数参数不能转为数值类型，具体错误为.${e.getMessage}")
              }
            }
            require(vocabSize > 0, "词汇数需要为大于0的正整数")

            val minTf = try {
              pretreatObj.get("minTf").getAsString.toDouble
            } catch {
              case e: Exception => throw new Exception(s"您输入词频参数有误，请输入数值类型," +
                s" 具体错误为：${e.getMessage}")
            }

            require(minTf >= 0, "最低词频需要大于等于0")

            val minDf = try {
              pretreatObj.get("minDf").getAsString.toDouble
            } catch {
              case e: Exception => throw new Exception(s"您输入的最低文档频率有误，请输入数值类型," +
                s" 具体错误为：${e.getMessage}")
            }

            require(minDf >= 0, "最低文档频率需要大于等于0")

            new CountWordVector(rawDataFrame)
              .setParams(CountWordVectorParamsName.inputCol, inputCol)
              .setParams(CountWordVectorParamsName.outputCol, outputCol)
              .setParams(CountWordVectorParamsName.vocabSize, vocabSize)
              .setParams(CountWordVectorParamsName.minTf, minTf)
              .setParams(CountWordVectorParamsName.minDf, minDf)
              .run()
              .data

          case "HashTF" =>
            val numFeatureString = try {
              pretreatObj.get("numFeature").getAsString.trim
            } catch {
              case e: Exception => throw new Exception(s"没有找到词汇数参数有误，具体错误为：${e.getMessage}")
            }

            val numFeature = if (numFeatureString contains '^') {
              try {
                numFeatureString.split('^').map(_.trim.toDouble).reduceLeft((d, s) => scala.math.pow(d, s)).toInt
              } catch {
                case e: Exception => throw new Exception(s"您输入词汇数参数表达式中包含指数运算符^，" +
                  s"但^两侧可能包含不能转为数值类型的字符，或您输入的指数过大超过了2^31，具体错误为.${e.getMessage}")
              }
            } else {
              try {
                numFeatureString.toDouble.toInt
              } catch {
                case e: Exception => throw new Exception(s"输入的词汇数参数不能转为数值类型，具体错误为.${e.getMessage}")
              }
            }

            require(numFeature > 0, "hash特征数需要为大于0的整数")

            new HashTF(rawDataFrame)
              .setParams(HashTFParamsName.numFeatures, numFeature)
              .setParams(HashTFParamsName.inputCol, inputCol)
              .setParams(HashTFParamsName.outputCol, outputCol)
              .run()
              .data

          case "WordToVector" =>
            val vocabSizeString = try {
              pretreatObj.get("vocabSize").getAsString.trim
            } catch {
              case e: Exception => throw new Exception(s"没有找到词汇数参数有误，具体错误为：${e.getMessage}")
            }

            val vocabSize = if (vocabSizeString contains '^') {
              try {
                vocabSizeString.split('^').map(_.trim.toDouble).reduceLeft((d, s) => scala.math.pow(d, s)).toInt
              } catch {
                case e: Exception => throw new Exception(s"您输入词汇数参数表达式中包含指数运算符^，" +
                  s"但^两侧可能包含不能转为数值类型的字符，或您输入的指数过大超过了2^31，具体错误为.${e.getMessage}")
              }
            } else {
              try {
                vocabSizeString.toDouble.toInt
              } catch {
                case e: Exception => throw new Exception(s"输入的词汇数参数不能转为数值类型，具体错误为.${e.getMessage}")
              }
            }
            require(vocabSize > 0, "词汇数需要大于0")

            val minCount = try {
              pretreatObj.get("minCount").getAsString.trim.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"参数最小词频有误，具体错误为：${e.getMessage}")
            }
            require(minCount > 0, "最小词频需要大于0")

            val stepSize = try {
              pretreatObj.get("stepSize").getAsString.trim.toDouble
            } catch {
              case e: Exception => throw new Exception(s"参数学习率有误，具体错误为：${e.getMessage}")
            }
            require(stepSize > 0, "学习率需要大于0")

            val numPartitions = try {
              pretreatObj.get("numPartitions").getAsString.trim.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"没有找到词汇数参数有误，具体错误为：${e.getMessage}")
            }
            require(numPartitions > 0, "并行度需要为正整数")

            val numIterations = try {
              pretreatObj.get("numIterations").getAsString.trim.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"没有找到词汇数参数有误，具体错误为：${e.getMessage}")
            }
            require(numIterations > 0, "模型迭代数需要为正整数")

            new WordToVector(rawDataFrame)
              .setParams(WordToVectorParamsName.inputCol, inputCol)
              .setParams(WordToVectorParamsName.outputCol, outputCol)
              .setParams(WordToVectorParamsName.vocabSize, vocabSize)
              .setParams(WordToVectorParamsName.stepSize, stepSize)
              .setParams(WordToVectorParamsName.minCount, minCount)
              .setParams(WordToVectorParamsName.numPartitions, numPartitions)
              .setParams(WordToVectorParamsName.numIterations, numIterations)
              .run()
              .data

          case "StopWordsRemover" =>
            val stopWordsFormat = pretreatObj.get("stopWordsFormat").getAsJsonObject.get("value").getAsString

            val stopWords = stopWordsFormat match {
              case "English" => StopWordsUtils.English
              case "Chinese" => StopWordsUtils.chinese
              case "byHand" =>
                val stopWordsArr = pretreatObj.get("stopWordsFormat").getAsJsonObject.get("stopWords").getAsJsonArray
                Array.range(0, stopWordsArr.size())
                  .map(i => stopWordsArr.get(i).getAsJsonObject.get("word").getAsString)
              case "byFile" =>
                try {
                  val path = pretreatObj.get("stopWordsFormat").getAsJsonObject.get("path").getAsString
                  val separator = pretreatObj.get("stopWordsFormat").getAsJsonObject.get("separator").getAsString
                  sc.textFile(path).collect().flatMap(_.split(separator).map(_.trim))
                } catch {
                  case e: Exception => throw new Exception(s"读取分词文件失败，具体信息${e.getMessage}")
                }
            }

            val caseSensitive = pretreatObj.get("caseSensitive").getAsString == "true"

            new StopWordsRmv(rawDataFrame)
              .setParams(StopWordsRemoverParamsName.inputCol, inputCol)
              .setParams(StopWordsRemoverParamsName.outputCol, inputCol)
              .setParams(StopWordsRemoverParamsName.caseSensitive, caseSensitive)
              .setParams(StopWordsRemoverParamsName.stopWords, stopWords)
              .run()
              .data

          case "NGram" =>
            val n = try {
              pretreatObj.get("n").getAsString.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"您输入的n值有误，请输入正整数值. ${e.getMessage}")
            }
            require(n > 0, "n值需要为正整数")

            new NGramMD(rawDataFrame)
              .setParams(NGramParamsName.inputCol, inputCol)
              .setParams(NGramParamsName.outputCol, outputCol)
              .setParams(NGramParamsName.n, n)
              .run()
              .data
        }

      case "numericDataTransform" => // 数值类特征转换 一个输入一个输出
        val inputCol = pretreatmentObj.get("inputCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
        val outputCol = pretreatmentObj.get("outputCol").getAsString

        val pretreatObj = pretreatmentObj.get("pretreatment").getAsJsonObject
        pretreatObj.get("value").getAsString match {
          case "Discretizer" =>
            val binningFormat = pretreatObj.get("binningFormat").getAsJsonObject.get("value").getAsString
            binningFormat match {
              case "byWidth" =>
                val phase = try {
                  pretreatObj.get("binningFormat").getAsJsonObject.get("phase").getAsString.toDouble
                } catch {
                  case e: Exception => throw new Exception(s"您输入的分箱起始点信息有误, ${e.getMessage}")
                }
                val width = try {
                  pretreatObj.get("binningFormat").getAsJsonObject.get("width").getAsString.toDouble
                } catch {
                  case e: Exception => throw new Exception(s"您输入的窗宽信息有误, ${e.getMessage}")
                }
                require(width > 0, "离散化箱子宽度需要大于0")

                new Discretizer(rawDataFrame)
                  .setParams(DiscretizerParams.inputCol, inputCol)
                  .setParams(DiscretizerParams.outputCol, outputCol)
                  .setParams(DiscretizerParams.discretizeFormat, "byWidth")
                  .setParams(DiscretizerParams.phase, phase)
                  .setParams(DiscretizerParams.width, width)
                  .run()
                  .data

              case "selfDefined" =>
                val arr = pretreatObj.get("binningFormat").getAsJsonObject.get("buckets").getAsJsonArray
                val buckets = try {
                  Array(0, arr.size()).map(i => arr.get(i).getAsJsonObject.get("bucket").getAsString.toDouble)
                } catch {
                  case e: Exception => throw new Exception(s"您输入的分隔信息有误, ${e.getMessage}")
                }

                val bucketsAddInfinity = try {
                  pretreatObj.get("binningFormat").getAsJsonObject.get("bucketsAddInfinity").getAsString == "true"
                } catch {
                  case e: Exception => throw new Exception(s"您输入的分隔信息有误, ${e.getMessage}")
                }

                new Discretizer(rawDataFrame)
                  .setParams(DiscretizerParams.inputCol, inputCol)
                  .setParams(DiscretizerParams.outputCol, outputCol)
                  .setParams(DiscretizerParams.discretizeFormat, "selfDefined")
                  .setParams(DiscretizerParams.buckets, buckets)
                  .setParams(DiscretizerParams.bucketsAddInfinity, bucketsAddInfinity)
                  .run()
                  .data
            }

          case "OneHotCoder" =>
            val dropLast = pretreatObj.get("dropLast").getAsString == "true"
            new OneHotCoder(rawDataFrame)
              .setParams(OneHotCoderParams.inputCol, inputCol)
              .setParams(OneHotCoderParams.outputCol, outputCol)
              .setParams(OneHotCoderParams.dropLast, dropLast)
              .run()
              .data

          case "IDFTransformer" =>
            val minDocFreq = try {
              pretreatObj.get("minDocFreq").getAsString.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"您输入的最小文档频率信息有误, ${e.getMessage}")
            }

            new IDFTransformer(rawDataFrame)
              .setParams(IDFTransformerParams.inputCol, inputCol)
              .setParams(IDFTransformerParams.outputCol, outputCol)
              .setParams(IDFTransformerParams.minDocFreq, minDocFreq) // @todo: 必须为Int
              .run()
              .data

          case "Vector2Indexer" =>
            val maxCategories = try {
              pretreatObj.get("maxCategories").getAsString.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"您输入的最少不同值数, ${e.getMessage}")
            }

            new VectorIndexerTransformer(rawDataFrame)
              .setParams(VectorIndexerParams.inputCol, inputCol)
              .setParams(VectorIndexerParams.outputCol, outputCol)
              .setParams(VectorIndexerParams.maxCategories, maxCategories) // @todo
              .run()
              .data

          case "PCATransformer" =>
            val p = try {
              pretreatObj.get("p").getAsString.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"您输入的主成分数, ${e.getMessage}")
            }
            require(p > 0, "主成分数需要为正整数")

            new PCATransformer(rawDataFrame)
              .setParams(PCAParams.inputCol, inputCol)
              .setParams(PCAParams.outputCol, outputCol)
              .setParams(PCAParams.p, p) // 需要小于向量长度
              .run()
              .data

          case "PlynExpansionTransformer" =>
            val degree = try {
              pretreatObj.get("degree").getAsString.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"您输入的展开式的幂, ${e.getMessage}")
            }
            require(degree > 0, "多项式的幂需要为正整数")

            new PlynExpansionTransformer(rawDataFrame)
              .setParams(PlynExpansionParams.inputCol, inputCol)
              .setParams(PlynExpansionParams.outputCol, outputCol)
              .setParams(PlynExpansionParams.degree, degree)
              .run()
              .data

          case "DCTTransformer" =>
            val inverse = try {
              pretreatObj.get("inverse").getAsString.trim == "true"
            } catch {
              case e: Exception => throw new Exception(s"您输入的展开式的幂, ${e.getMessage}")
            }

            new DCTTransformer(rawDataFrame)
              .setParams(DCTParams.inputCol, inputCol)
              .setParams(DCTParams.outputCol, outputCol)
              .setParams(DCTParams.inverse, inverse)
              .run()
              .data
        }

      case "numericScale" => // 数量尺度变换
        val inputCol = pretreatmentObj.get("inputCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
        val outputCol = pretreatmentObj.get("outputCol").getAsString

        val pretreatObj = pretreatmentObj.get("pretreatment").getAsJsonObject
        pretreatObj.get("value").getAsString match {
          case "NormalizerTransformer" => // 按行正则化
            val p = try {
              pretreatObj.get("p").getAsString
            } catch {
              case e: Exception => throw new Exception(s"您输入的正则化阶数信息有误, 异常信息${e.getMessage}")
            }

            new NormalizerTransformer(rawDataFrame)
              .setParams(NormalizerParams.inputCol, inputCol)
              .setParams(NormalizerParams.outputCol, outputCol)
              .setParams(NormalizerParams.p, p)
              .run()
              .data

          case "scale" =>
            val scaleFormatObj = pretreatObj.get("scaleFormat").getAsJsonObject
            scaleFormatObj.get("value").getAsString match {
              case "StandardScaleTransformer" =>
                val withMean = scaleFormatObj.get("withMean").getAsString == "true"
                val withStd = scaleFormatObj.get("withStd").getAsString == "true"

                new StandardScaleTransformer(rawDataFrame)
                  .setParams(StandardScaleParam.inputCol, inputCol)
                  .setParams(StandardScaleParam.outputCol, outputCol)
                  .setParams(StandardScaleParam.withStd, withStd)
                  .setParams(StandardScaleParam.withMean, withMean)
                  .run()
                  .data

              case "MinMaxScaleTransformer" =>
                val min = try {
                  scaleFormatObj.get("min").getAsString.trim.toDouble
                } catch {
                  case e: Exception => throw new Exception(s"请输入映射区间的最小值参数信息有误，${e.getMessage}")
                }
                val max = try {
                  scaleFormatObj.get("max").getAsString.trim.toDouble
                } catch {
                  case e: Exception => throw new Exception(s"请输入映射区间的最小值参数信息有误，${e.getMessage}")
                }

                new MinMaxScaleTransformer(rawDataFrame)
                  .setParams(MinMaxScaleParam.inputCol, inputCol)
                  .setParams(MinMaxScaleParam.outputCol, outputCol)
                  .setParams(MinMaxScaleParam.min, min)
                  .setParams(MinMaxScaleParam.max, max)
                  .run()
                  .data
            }

          case "ElementProduct" =>
            val vectorSize = rawDataFrame.select(inputCol).head.getAs[Vector](0).size

            val transformingArray = pretreatObj.get("weights").getAsJsonArray
            val weights = Array.range(0, transformingArray.size())
              .map(i => transformingArray.get(i).getAsJsonObject.get("weight").getAsString.toDouble)

            require(vectorSize == weights.length, "您输入的权重需要和向量长度保持一致")

            new ElementProduct(rawDataFrame)
              .setParams(ElementProductParams.inputCol, inputCol)
              .setParams(ElementProductParams.outputCol, outputCol)
              .setParams(ElementProductParams.weight, weights)
              .run()
              .data
        }

      case "attributesWithNumeric" => // 数值类型和属性类型互转
        val inputCol = pretreatmentObj.get("inputCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
        val outputCol = pretreatmentObj.get("outputCol").getAsString

        val pretreatObj = pretreatmentObj.get("pretreatment").getAsJsonObject
        pretreatObj.get("value").getAsString match {
          case "StringIndexTransformer" =>
            new StringIndexTransformer(rawDataFrame)
              .setParams(StringIndexParams.inputCol, inputCol)
              .setParams(StringIndexParams.outputCol, outputCol)
              .run()
              .data

          case "IndexerStringTransformer" =>
            val labelsArr = pretreatmentObj.get("labels").getAsJsonArray
            val labels = try {
              Array.range(0, labelsArr.size())
                .map(i => labelsArr.get(i).getAsJsonObject.get("label").getAsString)
            } catch {
              case e: Exception => throw new Exception(s"您输入的标签信息有误, ${e.getMessage}")
            }
            new IndexerStringTransformer(rawDataFrame)
              .setParams(IndexToStringParams.inputCol, inputCol)
              .setParams(IndexToStringParams.outputCol, outputCol)
              .setParams(IndexToStringParams.labels, labels)
              .run()
              .data
        }


      case "featureSelect" => // 特征选择, 可能多列输入一列输出
        val pretreatObj = pretreatmentObj.get("pretreatment").getAsJsonObject
        pretreatObj.get("value").getAsString match {
          case "VectorIndices" =>
            val inputCol = pretreatObj.get("inputCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
            val outputCol = pretreatObj.get("outputCol").getAsString
            val indicesArr = pretreatObj.get("indices").getAsJsonArray
            val indices = Array.range(0, indicesArr.size())
              .map(i => indicesArr.get(i).getAsJsonObject.get("indice").getAsString.toDouble.toInt)

            new VectorIndices(rawDataFrame)
              .setParams(VectorIndicesParams.inputCol, inputCol)
              .setParams(VectorIndicesParams.outputCol, outputCol)
              .setParams(VectorIndicesParams.indices, indices)
              .run()
              .data

          case "ChiFeatureSqSelector" =>
            val inputCol = pretreatObj.get("inputCol").getAsJsonArray.get(0).getAsJsonObject.get("name").getAsString
            val outputCol = pretreatObj.get("outputCol").getAsString
            val topFeatureNums = try {
              pretreatObj.get("topFeatureNums").getAsString.toDouble.toInt
            } catch {
              case e: Exception => throw new Exception(s"您输入的特征数有误，${e.getMessage}")
            }

            new ChiFeatureSqSelector(rawDataFrame)
              .setParams(ChiFeatureSqSelectorParams.inputCol, inputCol)
              .setParams(ChiFeatureSqSelectorParams.outputCol, outputCol)
              .setParams(ChiFeatureSqSelectorParams.topFeatureNums, topFeatureNums)
              .run()
              .data

          case "VectorAssembleTransformer" =>
            val inputColsArr = pretreatObj.get("inputCol").getAsJsonArray
            val inputCols = Array.range(0, inputColsArr.size())
              .map(i => inputColsArr.get(i).getAsJsonObject.get("name").getAsString)
            val outputCol = pretreatObj.get("outputCol").getAsString

            new VectorAssembleTransformer(rawDataFrame)
              .setParams(VectorAssembleParams.inputCol, inputCols)
              .setParams(VectorAssembleParams.outputCol, outputCol)
              .run()
              .data
        }

    }


  }
}
