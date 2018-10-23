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


  /**
    * 根据dim维的边界拓展到第dim+1维
    *
    * @param dim              第dim维
    * @param initial_boundary 第dim+1维的边界初始值，一般选择初始节点的第dim+1维的值
    * @param new_sub_graph    适用于更高维度的子图
    * @return ((dim+1维下界, dim+1维上界), 该维度在边界内的子图)
    */
  def growth(dim: Int, initial_boundary: Int, new_sub_graph: ArrayBuffer[Seq[Int]])
  : ((Int, Int), ArrayBuffer[Seq[Int]]) = {
    // dim可能的取值, 每个取值对应的dim + 1取值
    val range4dim_plus1 = new_sub_graph.map(e => (util.Try(e(dim)).getOrElse(-1), e(dim + 1))).groupBy(_._1)

    val (left_boundary4dimP1, right_boundary4dimP1) = range4dim_plus1.keySet.map {
      dimValue =>
        val range4_dimValue_dimPlus1 = range4dim_plus1(dimValue).map(_._2)
        var leftReach = false
        var rightReach = false

        // dim可能的取值, 每个取值对应的dim + 1取值
        var left_boundary = initial_boundary
        var right_boundary = initial_boundary

        var i = 0
        while (!(leftReach && rightReach)) {
          /** 向左直到遇到坑 */
          if (range4_dimValue_dimPlus1 contains initial_boundary - i) {
            left_boundary = initial_boundary - i
          } else {
            leftReach = true
          }

          /** 向右直到遇到坑 */
          if (range4_dimValue_dimPlus1 contains initial_boundary + i) {
            right_boundary = initial_boundary + i
          } else {
            rightReach = true
          }

          i += 1
        }
        (left_boundary, right_boundary)
    }.reduce[(Int, Int)] {
      case ((left_boundary1, right_boundary1), (left_boundary2, right_boundary2)) =>
        (scala.math.max(left_boundary1, left_boundary2), scala.math.min(right_boundary1, right_boundary2))
    }

    (
      (left_boundary4dimP1, right_boundary4dimP1),
      new_sub_graph.filter(elem => elem(dim + 1) >= left_boundary4dimP1 && elem(dim + 1) <= right_boundary4dimP1)
    )
  }


  /**
    * 寻找当前子图的最小表示以及未表示的子图
    * ----
    * 算法:
    * 1）从当前子图的第一个节点为对象, K为节点的最高维度,
    * 当前维度k = 0，待遍历子图graph2traverse = subGraph,
    * 维度上下界dimRange = "Array.empty[(Int,Seq[Int], Seq[Int])]"，元素依次为：维度，下界，上界，注意上下界均为闭区间；
    * 3）在维度k上从当前节点在graph2traverse内向两个方向出发，遇到“坑”时停止，找到上下界，dimRange += (k, 下界，上界)，
    * graph2traverse = graph2traverse.filter(element => element >= 下界 && element <= 上界)；
    * 4）k += 1，重复2和3直到k = K - 1时停止；
    * 5）过滤未在上下界之内的子图作为未表示的子图，return (上下界, 未表示的子图)。
    *
    * @param subGraph 当前子图，注意不能为空
    *                 (最小表示，当前子图和最小表示的差集）
    */
  def miniRepresentFromOneNode(node: Seq[Int], subGraph: ArrayBuffer[Seq[Int]]): Array[(Int, Int)] = {

    val K = node.length
    var k = -1
    var graph2traverse = subGraph
    val dimRange = mutable.ArrayBuilder.make[(Int, Int)]()
    while (k < K - 1) {
      /** 根据k维上下界和graph2traverse寻找k + 1维的上下界 --k = -1时表示初始节点 */
      val ((left_bound, right_bound), newGraph) = growth(k, node(k + 1), graph2traverse)
      dimRange += Tuple2(left_bound, right_bound)
      graph2traverse = newGraph
      k += 1
    }
    dimRange.result()
  }

    /** 根据k维上下界和graph2traverse寻找k + 1维的上下界 --k = -1时表示初始节点 */
    def upSearch(
                  node: Seq[Int],
                  low_Boundary: Seq[Int],
                  up_Boundary: Seq[Int],
                  subGraph: ArrayBuffer[Seq[Int]]
                ) = {
      val dim = low_Boundary.length
      val newSubGraph = subGraph.filter(
        node =>
          node.take(dim).zip(low_Boundary).zip(up_Boundary).foldLeft(true) {
            case (inRange, ((value, low_bd), up_bd)) =>
              inRange && low_bd <= value && value <= up_bd
          })


      /** k维度的边界 --分别从这些值出发向两侧寻找最长的连续序列 */
      ArrayBuffer.range(low_Boundary.last, up_Boundary.last).map {
        i =>

      }
      newSubGraph.map(_.apply(dim))
    }
//
//  def aa() = {
//    val (low_Boundary: Seq[Int], up_Boundary: Seq[Int]) = (Seq(1, 2, 3), Seq(3, 4, 5)) // 3
//
//
//    var candidate4node = subGraph
//    // 依次遍历所有的维度, 维度按次序递增, 找到对于该节点最大的覆盖
//    for (dims <- node.indices) {
//      candidate4node = candidate4node.filter(other => other.drop(dims) == node.drop(dims))
//      val uu = candidate4node.map(each => each.slice(dims, dims + 1).head) // 有序的
//      var start = node.slice(dims, dims + 1).head
//      val index = uu.indexOf(start)
//      var end = node.apply(dims)
//      var flag = true
//      // 往两个方向各自走, 走到两个方向都遇到坑为止(非连续)
//      var u = 0
//      while (flag) {
//        if (index - u >= 0) {
//          val left = uu(index - u)
//          if (start - left == 1) {
//            start -= 1
//          } else {
//            flag = false
//          }
//        } else {
//          flag = false
//        }
//        u += 1
//      }
//
//      flag = true
//      u = 0
//      while (flag) {
//        if (index + u < uu.length) {
//          val right = uu(index + u)
//          if (right - end == 1) {
//            start -= 1
//          } else {
//            flag = false
//          }
//        } else {
//          flag = false
//        }
//        u += 1
//      }
//
//    }
//
//
//    }


}
