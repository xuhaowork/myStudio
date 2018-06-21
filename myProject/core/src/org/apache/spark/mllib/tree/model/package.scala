package org.apache.spark.mllib.tree

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.configuration.Algo.Classification
import org.apache.spark.mllib.tree.configuration.FeatureType._

package object model {

  class NodeWithProbPredict(val id2: Int,
                            var predict2: Predict,
                            var impurity2: Double,
                            var isLeaf2: Boolean,
                            var split2: Option[Split],
                            var leftNode2: Option[NodeWithProbPredict],
                            var rightNode2: Option[NodeWithProbPredict],
                            var stats2: Option[InformationGainStats]) extends Node(
    id2, predict2, impurity2, isLeaf2, split2, leftNode2, rightNode2, stats2) {
    /**
      * 加入预测概率的函数
      *
      * @param features 特征向量
      * @return
      */
    def predictProb(features: Vector): Double = {
      if (isLeaf) {
//        println("leaf:", predict2.toString())
        predict2.prob
      } else {
        if (split2.get.featureType == Continuous) {
          if (features(split2.get.feature) <= split2.get.threshold) {
//            println("leftNode:", leftNode2.get.toString())
            leftNode2.get.toNodeWithProbPredict.predictProb(features)
          } else {
//            println("rightNode:", rightNode2.get.toString())
            rightNode2.get.toNodeWithProbPredict.predictProb(features)
          }
        } else {
          if (split2.get.categories.contains(features(split2.get.feature))) {
//            println("leftNode:", leftNode2.get.toString())
            leftNode2.get.toNodeWithProbPredict.predictProb(features)
          } else {
//            println("rightNode:", rightNode2.get.toString())
            rightNode2.get.toNodeWithProbPredict.predictProb(features)
          }
        }
      }
    }

  }

  implicit class Node2NodeWithProbPredict(val node: Node) {
    def toNodeWithProbPredict: NodeWithProbPredict = {
      if (node.isLeaf) {
        new NodeWithProbPredict(node.id,
          node.predict,
          node.impurity,
          node.isLeaf,
          None,
          None,
          None,
          node.stats)
      } else {
        new NodeWithProbPredict(node.id,
          node.predict,
          node.impurity,
          node.isLeaf,
          node.split,
          Some(node.leftNode.get.toNodeWithProbPredict),
          Some(node.rightNode.get.toNodeWithProbPredict),
          node.stats)
      }

    }
  }


  /**
    * 隐式转换，给[[DecisionTreeModelImpl]]赋予一个[[predictProb]]方法
    *
    * @param treeEnsembleModel
    */
  implicit class DecisionTreeModelImpl(val treeEnsembleModel: DecisionTreeModel) {
    def predictProb(features: Vector): Double = {
      require(treeEnsembleModel.algo == Classification, "概率形式输出要求模型必须为分类模型。")
      val topNode = treeEnsembleModel.topNode

      topNode.toNodeWithProbPredict.predictProb(features)
    }


  }


}
