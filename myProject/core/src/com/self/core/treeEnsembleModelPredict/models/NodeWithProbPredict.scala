package com.self.core.treeEnsembleModelPredict.models

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.tree.model._

/**
  * 提供一个带节点概率输出的Node
  *
  * 一个有节点概率输出的Node类
  * ----
  * 提供节点概率输出方法 @define [[predictWithProb]]
  *
  * @param id2        integer node id, from 1
  * @param predict2   predicted value at the node
  * @param impurity2  current node impurity
  * @param isLeaf2    whether the node is a leaf
  * @param split2     split to calculate left and right nodes
  * @param leftNode2  left child
  * @param rightNode2 right child
  * @param stats2     information gain stats
  */
class NodeWithProbPredict(val id2: Int,
                          var predict2: Predict,
                          var impurity2: Double,
                          var isLeaf2: Boolean,
                          var split2: Option[Split],
                          var leftNode2: Option[NodeWithProbPredict],
                          var rightNode2: Option[NodeWithProbPredict],
                          var stats2: Option[InformationGainStats]) extends Node(
  id2, predict2, impurity2, isLeaf2, split2, leftNode2, rightNode2, stats2) {

  /** 引入隐式转换[[ImplicitForDecisionTree]] */

  import com.zzjz.deepinsight.core.treeEnsembleModelPredict.models.ImplicitForDecisionTree._

  /**
    * 加入预测概率的函数
    *
    * @param features 特征向量
    * @return 输出两个元素的元组：(预测值, 概率)
    */
  def predictWithProb(features: Vector): (Double, Double) = {
    if (isLeaf) {
      (predict2.predict, predict2.prob)
    } else {
      if (split2.get.featureType == Continuous) {
        if (features(split2.get.feature) <= split2.get.threshold) {
          leftNode2.get.toNodeWithProbPredict.predictWithProb(features)
        } else {
          rightNode2.get.toNodeWithProbPredict.predictWithProb(features)
        }
      } else {
        if (split2.get.categories.contains(features(split2.get.feature))) {
          leftNode2.get.toNodeWithProbPredict.predictWithProb(features)
        } else {
          rightNode2.get.toNodeWithProbPredict.predictWithProb(features)
        }
      }
    }
  }


}