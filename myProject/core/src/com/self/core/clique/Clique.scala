package com.self.core.clique

import com.self.core.baseApp.myAPP
import com.self.core.clique.models.{CliqueUtils, FreqItem, MinMaxStandard}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer, Map => mutableMap}

/** CLIQUE聚类算法 */
object Clique extends myAPP {
  def createData(): Unit = {
    val dt = Seq(
      (5.1, 3.5, 1.4, 0.2),
      (4.9, 3.0, 1.4, 0.2),
      (4.7, 3.2, 1.3, 0.2),
      (4.6, 3.1, 1.5, 0.2),
      (5.0, 3.6, 1.4, 0.2),
      (5.4, 3.9, 1.7, 0.4),
      (4.6, 3.4, 1.4, 0.3),
      (5.0, 3.4, 1.5, 0.2),
      (4.4, 2.9, 1.4, 0.2),
      (4.9, 3.1, 1.5, 0.1),
      (5.4, 3.7, 1.5, 0.2),
      (4.8, 3.4, 1.6, 0.2),
      (4.8, 3.0, 1.4, 0.1),
      (4.3, 3.0, 1.1, 0.1),
      (5.8, 4.0, 1.2, 0.2),
      (5.7, 4.4, 1.5, 0.4),
      (5.4, 3.9, 1.3, 0.4),
      (5.1, 3.5, 1.4, 0.3),
      (5.7, 3.8, 1.7, 0.3),
      (5.1, 3.8, 1.5, 0.3),
      (5.4, 3.4, 1.7, 0.2),
      (5.1, 3.7, 1.5, 0.4),
      (4.6, 3.6, 1.0, 0.2),
      (5.1, 3.3, 1.7, 0.5),
      (4.8, 3.4, 1.9, 0.2),
      (5.0, 3.0, 1.6, 0.2),
      (5.0, 3.4, 1.6, 0.4),
      (5.2, 3.5, 1.5, 0.2),
      (5.2, 3.4, 1.4, 0.2),
      (4.7, 3.2, 1.6, 0.2),
      (4.8, 3.1, 1.6, 0.2),
      (5.4, 3.4, 1.5, 0.4),
      (5.2, 4.1, 1.5, 0.1),
      (5.5, 4.2, 1.4, 0.2),
      (4.9, 3.1, 1.5, 0.2),
      (5.0, 3.2, 1.2, 0.2),
      (5.5, 3.5, 1.3, 0.2),
      (4.9, 3.6, 1.4, 0.1),
      (4.4, 3.0, 1.3, 0.2),
      (5.1, 3.4, 1.5, 0.2),
      (5.0, 3.5, 1.3, 0.3),
      (4.5, 2.3, 1.3, 0.3),
      (4.4, 3.2, 1.3, 0.2),
      (5.0, 3.5, 1.6, 0.6),
      (5.1, 3.8, 1.9, 0.4),
      (4.8, 3.0, 1.4, 0.3),
      (5.1, 3.8, 1.6, 0.2),
      (4.6, 3.2, 1.4, 0.2),
      (5.3, 3.7, 1.5, 0.2),
      (5.0, 3.3, 1.4, 0.2),
      (7.0, 3.2, 4.7, 1.4),
      (6.4, 3.2, 4.5, 1.5),
      (6.9, 3.1, 4.9, 1.5),
      (5.5, 2.3, 4.0, 1.3),
      (6.5, 2.8, 4.6, 1.5),
      (5.7, 2.8, 4.5, 1.3),
      (6.3, 3.3, 4.7, 1.6),
      (4.9, 2.4, 3.3, 1.0),
      (6.6, 2.9, 4.6, 1.3),
      (5.2, 2.7, 3.9, 1.4),
      (5.0, 2.0, 3.5, 1.0),
      (5.9, 3.0, 4.2, 1.5),
      (6.0, 2.2, 4.0, 1.0),
      (6.1, 2.9, 4.7, 1.4),
      (5.6, 2.9, 3.6, 1.3),
      (6.7, 3.1, 4.4, 1.4),
      (5.6, 3.0, 4.5, 1.5),
      (5.8, 2.7, 4.1, 1.0),
      (6.2, 2.2, 4.5, 1.5),
      (5.6, 2.5, 3.9, 1.1),
      (5.9, 3.2, 4.8, 1.8),
      (6.1, 2.8, 4.0, 1.3),
      (6.3, 2.5, 4.9, 1.5),
      (6.1, 2.8, 4.7, 1.2),
      (6.4, 2.9, 4.3, 1.3),
      (6.6, 3.0, 4.4, 1.4),
      (6.8, 2.8, 4.8, 1.4),
      (6.7, 3.0, 5.0, 1.7),
      (6.0, 2.9, 4.5, 1.5),
      (5.7, 2.6, 3.5, 1.0),
      (5.5, 2.4, 3.8, 1.1),
      (5.5, 2.4, 3.7, 1.0),
      (5.8, 2.7, 3.9, 1.2),
      (6.0, 2.7, 5.1, 1.6),
      (5.4, 3.0, 4.5, 1.5),
      (6.0, 3.4, 4.5, 1.6),
      (6.7, 3.1, 4.7, 1.5),
      (6.3, 2.3, 4.4, 1.3),
      (5.6, 3.0, 4.1, 1.3),
      (5.5, 2.5, 4.0, 1.3),
      (5.5, 2.6, 4.4, 1.2),
      (6.1, 3.0, 4.6, 1.4),
      (5.8, 2.6, 4.0, 1.2),
      (5.0, 2.3, 3.3, 1.0),
      (5.6, 2.7, 4.2, 1.3),
      (5.7, 3.0, 4.2, 1.2),
      (5.7, 2.9, 4.2, 1.3),
      (6.2, 2.9, 4.3, 1.3),
      (5.1, 2.5, 3.0, 1.1),
      (5.7, 2.8, 4.1, 1.3),
      (6.3, 3.3, 6.0, 2.5),
      (5.8, 2.7, 5.1, 1.9),
      (7.1, 3.0, 5.9, 2.1),
      (6.3, 2.9, 5.6, 1.8),
      (6.5, 3.0, 5.8, 2.2),
      (7.6, 3.0, 6.6, 2.1),
      (4.9, 2.5, 4.5, 1.7),
      (7.3, 2.9, 6.3, 1.8),
      (6.7, 2.5, 5.8, 1.8),
      (7.2, 3.6, 6.1, 2.5),
      (6.5, 3.2, 5.1, 2.0),
      (6.4, 2.7, 5.3, 1.9),
      (6.8, 3.0, 5.5, 2.1),
      (5.7, 2.5, 5.0, 2.0),
      (5.8, 2.8, 5.1, 2.4),
      (6.4, 3.2, 5.3, 2.3),
      (6.5, 3.0, 5.5, 1.8),
      (7.7, 3.8, 6.7, 2.2),
      (7.7, 2.6, 6.9, 2.3),
      (6.0, 2.2, 5.0, 1.5),
      (6.9, 3.2, 5.7, 2.3),
      (5.6, 2.8, 4.9, 2.0),
      (7.7, 2.8, 6.7, 2.0),
      (6.3, 2.7, 4.9, 1.8),
      (6.7, 3.3, 5.7, 2.1),
      (7.2, 3.2, 6.0, 1.8),
      (6.2, 2.8, 4.8, 1.8),
      (6.1, 3.0, 4.9, 1.8),
      (6.4, 2.8, 5.6, 2.1),
      (7.2, 3.0, 5.8, 1.6),
      (7.4, 2.8, 6.1, 1.9),
      (7.9, 3.8, 6.4, 2.0),
      (6.4, 2.8, 5.6, 2.2),
      (6.3, 2.8, 5.1, 1.5),
      (6.1, 2.6, 5.6, 1.4),
      (7.7, 3.0, 6.1, 2.3),
      (6.3, 3.4, 5.6, 2.4),
      (6.4, 3.1, 5.5, 1.8),
      (6.0, 3.0, 4.8, 1.8),
      (6.9, 3.1, 5.4, 2.1),
      (6.7, 3.1, 5.6, 2.4),
      (6.9, 3.1, 5.1, 2.3),
      (5.8, 2.7, 5.1, 1.9),
      (6.8, 3.2, 5.9, 2.3),
      (6.7, 3.3, 5.7, 2.5),
      (6.7, 3.0, 5.2, 2.3),
      (6.3, 2.5, 5.0, 1.9),
      (6.5, 3.0, 5.2, 2.0),
      (6.2, 3.4, 5.4, 2.3),
      (5.9, 3.0, 5.1, 1.8)
    )

    val newDataFrame = sqlc.createDataFrame(dt).toDF("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width")

    println("鸢尾花部分维度数据")
    newDataFrame.show()
    outputrdd.put("iris", newDataFrame)
  }


  def test(): Unit = {
    /** 创建模拟数据 */
    createData()

    import org.apache.spark.sql.functions.{col, count, lit}
    import org.apache.spark.sql.{DataFrame, NullableFunctions}

    val z1 = z

    /** 参数处理 */
    val data: DataFrame = z1.rdd("iris").asInstanceOf[DataFrame]

    val features: Array[String] = Array("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width")
    val featuresNum = features.length
    require(featuresNum >= 2, "")

    val cellsNum = 100
    require(cellsNum >= 5, "拆分单元数至少为5, 否则结果没有聚类意义")
    val splitCellsNum = scala.math.pow(cellsNum, featuresNum).toInt
    require(scala.math.pow(cellsNum, featuresNum) <= (1 << 30), s"您选择拆分单元数'$cellsNum'过大, " +
      s"这样会拆分出接近'$splitCellsNum'个单元, 超出了目前算子的处理能力, 请降低数据维度或调小拆分单元数")

    val minDenseThreshold = 2
    require(minDenseThreshold > 0, "最小密集单元数至少应大于0, 否则全部数单元格都作为密集单元此时没有意义，另外注意如果最小密集单元数如果过小可能会导致性能问题")

    /** step1. min-max归一化 */
    val standardCol = "standardCol"
    val unStandardCol = "unStandardCol"
    val standardModel = MinMaxStandard.train(data, features)
      .setStandardCol(standardCol)
      .setUnStandardCol(unStandardCol)
    val standardDF = standardModel.standardize(data)

    standardDF.show()

    /** step2. 等宽拆分单元格 */
    def splitCellUdf(cellsNum: Int) = NullableFunctions.udf(
      (values: Seq[Double]) =>
        values.map(d => (d * cellsNum).toInt)
    )

    val splitCellsDF = standardDF.select(splitCellUdf(cellsNum).apply(col(standardCol)).as("splitCells"))

    splitCellsDF.show()

    /**
      * 自顶向下的统计频次 --这样能最大限度利用上一个维度的频次统计结果
      * 优点:
      * 1)对于高维数据: 避免了lazy模型下, 为了不重复执行父rdd的stage要频繁cache --这个只cache一次最终结果就行了
      * 2)和原算法不一样, 因为spark分布式很擅长频次统计, 采用原来自下向上的频繁过滤 + filter + 频次统计反而不能提高效率
      * 3)虽然这里统计频次方式和原算法不一样, 但其他过程尽量按原算法来的, 由于不是采用近似, 精度只会比原算法高.
      *
      * @note 为什么不采用原算法?
      *       原算法这样从下至上是为了增加效率, 对次迭代, 始终维护一个候选集合C_t和有效数据集data
      *       1)data = data.filter(data in C_t); 统计data每个单元格的分布上的分布, collect获得稠密单元格D_t
      *       2)根据D_t,在 t+1 维度上延展, collect获得C_(t+1)
      *       3)递归1和2直至候选集为空
      *       ----
      *       1)这样的算法在非分布式的编程时可以的, 因为数据都是加载在缓存中;
      *       2)在spark中每次filter其实都是向最初的RDD或最近Cache的RDD拿数据, 如果data仅在递归开始前cache一次,
      *       那在第t次依然需要重复前面t-1次的filter过程, 如果每一步cache, 数据较大时性能更成问题.
      *       3)因此对于分布式来说全部分组统计求频率反而不是大问题, 每一步filter + 分组统计, 才是问题
      *       4)基于以上问题, 剪枝也不需要了, 在CLIQUE算法中剪枝是为了尽量减少自下向上的单元格, 但这是一个近似过程,
      *       会导致部分有效的单元格被减掉, 被后续的一些改进算法所抛弃, 由于spark频次统计很在行, 因此这里抛弃剪枝.
      */
    var frequencyRdd: RDD[(Seq[Int], (Long, ArrayBuffer[FreqItem]))] =
      splitCellsDF.groupBy("splitCells").agg(count(lit(1)).as("frequency")).rdd.map {
        row =>
          // 最初keys的长度为featuresNum
          val keys = row.getAs[Seq[Int]](0)
          // 父节点的key, (父节点统计的频次, 子节点的统计结果)
          (keys, (row.getAs[Long](1), ArrayBuffer.empty[FreqItem]))
      }

    // 上卷一个维度直至第一个维度
    var i = featuresNum
    while (i > 1) {
      frequencyRdd = frequencyRdd.map {
        case (keys, (frequency, children)) =>
          (keys.dropRight(1), (frequency, ArrayBuffer(FreqItem(keys.last, frequency, children))))
      }.reduceByKey {
        case ((frequency1, item1), (frequency2, item2)) =>
          (frequency1 + frequency2, item1 ++ item2)
      }
      i -= 1
    }

    println("上卷后")
    frequencyRdd.take(100).foreach(println)

    println("i为", i)

    /**
      * 自下向上的找到在尽可能大的维度上找到密集单元格
      */
    var denseCellsRDD: RDD[(Seq[Int], (Long, ArrayBuffer[FreqItem]))] = null

    var cellsCount: Long = 0
    val firstCandidate = frequencyRdd.filter {
      case (_, (frequency, _)) =>
        frequency >= minDenseThreshold
    }
    firstCandidate.cache()
    var candidateDenseCells: RDD[(Seq[Int], (Long, ArrayBuffer[FreqItem]))] = firstCandidate

    var flag = true
    while (i <= featuresNum && flag) {
      candidateDenseCells = candidateDenseCells.filter {
        case (_, (frequency, _)) =>
          frequency >= minDenseThreshold
      }

      cellsCount = candidateDenseCells.count()
      if (cellsCount > 0) {
        denseCellsRDD = candidateDenseCells
        // 下钻一个维度
        candidateDenseCells = candidateDenseCells.flatMap {
          case (keys, (_, children)) =>
            children.map {
              freqItem =>
                (keys :+ freqItem.key, (freqItem.frequency, children))
            }
        }
      } else {
        flag = false
      }
      i += 1
    }

    denseCellsRDD.foreach(println)

    val denseCells: Array[Seq[Int]] = try {
      denseCellsRDD.keys.collect()
    } catch {
      case e: Exception => throw new Exception("将稠密单元格本地化时失败, 有可能是稠密单元数过多导致传输过程中超过了" +
        "akkaFrameSize, 如果是此情况, 您可以调大最小密集单元数来控制该问题或调大资源配置中的akkaFrameSize. " +
        s"具体异常为: ${e.getMessage}")
    }

    /** 基于深度优先的遍历 */
    CliqueUtils.dfs(denseCells: Array[Seq[Int]])


  }

  override def run(): Unit = {

    //    test()


    //    val memoryMap = mutableMap.empty[Seq[Int], Long]
    //    memoryMap += (Seq(0) -> 1L)
    //    memoryMap += (Seq(1) -> 2L)
    //    memoryMap += (Seq(0) -> 3L)
    //
    //    println(memoryMap)

    /** 稠密单元 */
    val cellsData =
      Array(
        Seq(0, 2),
        Seq(1, 2),
        Seq(1, 3),
        Seq(2, 1),
        Seq(2, 2),
        Seq(2, 3),
        Seq(4, 2),
        Seq(4, 3),
        Seq(5, 2),
        Seq(5, 3),
        Seq(6, 1),
        Seq(6, 2),
        Seq(6, 3),
        Seq(7, 1),
        Seq(7, 2)
      )

    CliqueUtils.hotPlot(cellsData, theme = "原数据的热力图")

    val res = CliqueUtils.dfs(cellsData: Array[Seq[Int]])
    res.foreach(println)

    CliqueUtils.hotPlot(res(0)._2, theme = "连通图1的热力图")

    CliqueUtils.hotPlot(res(1)._2, theme = "连通图2的热力图")

    val (subGraph1, subGraph): (Int, ArrayBuffer[Seq[Int]]) = res(0)


    subGraph.foreach {
      node =>
        var candidate4node = subGraph
        for (i <- node.indices) {
          candidate4node = candidate4node.filter(other => other.drop(1) == node.drop(1))
        }
    }

    // 未被遍历的node
    var candidateNodes = subGraph
    while(candidateNodes.nonEmpty) {
      // 任选一个节点
      val node = candidateNodes.head

      var candidate4node = subGraph
      // 依次遍历所有的维度, 维度按次序递增, 找到对于该节点最大的覆盖
      for (dims <- node.indices) {
        candidate4node = candidate4node.filter(other => other.drop(dims) == node.drop(dims))
        val uu = candidate4node.map(each => each.slice(dims, dims + 1).head) // 有序的
        var start = node.slice(dims, dims + 1).head
        val index = uu.indexOf(start)
        var end = node.apply(dims)
        var flag = true
        // 往两个方向各自走, 走到两个方向都遇到坑为止(非连续)
        var u = 0
        while(flag) {
          if(index - u >= 0) {
            val left = uu(index - u)
            if(start - left == 1){
              start -= 1
            } else {
              flag = false
            }
          } else {
            flag = false
          }
          u += 1
        }

        flag = true
        u = 0
        while(flag) {
          if(index + u < uu.length) {
            val right = uu(index + u)
            if(right - end == 1){
              start -= 1
            } else {
              flag = false
            }
          } else {
            flag = false
          }
          u += 1
        }


      }



    }







  }

}
