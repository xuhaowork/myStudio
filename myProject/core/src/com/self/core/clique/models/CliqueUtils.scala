package com.self.core.clique.models

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CliqueUtils extends Serializable {
  // 判定两点是否相邻 --当且仅当曼哈顿距离为1
  private[clique] def isNeighbor(v1: Seq[Int], v2: Seq[Int]): Boolean =
    v1.zip(v2).map { case (axis1, axis2) => scala.math.abs(axis1 - axis2) }.sum == 1


  /**
    * 基于深度优先的遍历
    * ----
    * 算法:
    * 从任意网格出发, 开启一个新的子图, 找到网格中和其相邻的网格, 找到后更新当前图,
    * 一直找直到找不到相邻网格时, 退回当前图的其他节点继续比遍历非本图的网格;
    * 当当前图所有节点遍历结束后, 重新选择任意一个未归属到任何图中的结点, 开启一个新图, 重复上述步骤, 直至没有任何未归属的点
    *
    * @param denseCells 初始的图数据
    * @return Array[(连通图Id, 连通图)]
    */
  def dfs(denseCells: Array[Seq[Int]]): Array[(Int, ArrayBuffer[Seq[Int]])] = {
    // 未知世界
    var noneInGridNodes = new mutable.ArrayBuffer[Seq[Int]](0) ++ denseCells
    // 已知知识
    val neighbors = mutable.ArrayBuilder.make[(Int, ArrayBuffer[Seq[Int]])]()
    // 知识块的id
    var subGraphId = 0
    // 临时已知的知识块
    var subGraph = new mutable.ArrayBuffer[Seq[Int]](0)
    // 遍历知识点直到没有可以学习的知识
    while (noneInGridNodes.nonEmpty) {
      // 从任意一个未归属的知识点开始, 当前子图清空, 变为包含第一个节点的子图
      var node = noneInGridNodes.head
      noneInGridNodes = noneInGridNodes.drop(1)
      subGraph = mutable.ArrayBuffer(node)

      var loopId = 0
      while (loopId < subGraph.size) {
        node = subGraph(loopId)
        // node向未知世界出发进行探究, 找到一个合适的就存到graph中, 从从未知世界删除,
        // 找不到就换graph的下一个上, 直到graph没有下一个成员
        subGraph ++= noneInGridNodes.filter {
          nd => isNeighbor(node, nd)
        }
        noneInGridNodes = noneInGridNodes.filter {
          nd => !isNeighbor(node, nd)
        }
        loopId += 1

      }
      // 将知识块纳入知识麾下
      neighbors += Tuple2(subGraphId, subGraph)
      // 更新知识块id
      subGraphId += 1
    }

    neighbors.result()
  }


  def hotPlot(cellsData: Seq[Seq[Int]], theme: String, multiply: Double = 1.0): Unit = {
    println("绘制热力图")
    import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
    import breeze.plot._

    // 这一段是需要的, 不知道为什么, 官网上也没有介绍
    val a = new BDV[Int](1 to 3 toArray)
    val b = new BDM[Int](3, 3, 1 to 9 toArray)

    /** 将稠密单元转为可以画图的breez.linalg.DenseMatrix */
    val range = 10

    val figure = Figure()
    figure.subplot(0) += image(img = new BDM[Double](range, range, Array.range(0, range).flatMap {
      x =>
        Range(0, range).map {
          y =>
            if (cellsData contains Seq(x, y)) multiply else 0.0
        }
    }), name = theme)
  }


}
