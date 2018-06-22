package com.self.core.treeEnsembleModelPredict.models

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.configuration.Algo.Classification
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Node}

/**
  * 决策树结点概率输出的隐式转换脚本
  * ----
  *
  * @define [[Node2NodeWithProbPredict ]] 用于[[NodeWithProbPredict]]中的[predictWithProb]方法
  * @define [[DecisionTreeModelImpl ]] 为[[DecisionTreeModel]]提供了一个可以输出结点概率的[predictWithProb]方法
  *
  */
object ImplicitForDecisionTree {

  /**
    * [[org.apache.spark.mllib.tree.model.Node]]转为[[NodeWithProbPredict]]的隐式转换
    *
    * 实例化需要递归实现
    *
    * @param node Node类
    */
  implicit class Node2NodeWithProbPredict(val node: Node) {
    /** 递归转换函数 */
    def toNodeWithProbPredict: NodeWithProbPredict = {
      if (node.isLeaf) {
        new NodeWithProbPredict(node.id,
          node.predict,
          node.impurity,
          node.isLeaf,
          node.split,
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
    * 隐式转换，给[[DecisionTreeModelImpl]]赋予一个[[predictWithProb]]方法
    *
    * @param treeEnsembleModel 原有的[[DecisionTreeModel]]
    */
  implicit class DecisionTreeModelImpl(val treeEnsembleModel: DecisionTreeModel) {

    /** 概率预测方法 */
    def predictWithProb(features: Vector): (Double, Double) = {
      require(treeEnsembleModel.algo == Classification, "概率形式输出要求模型必须为分类模型。")
      val topNode = treeEnsembleModel.topNode

      topNode.toNodeWithProbPredict.predictWithProb(features)
    }
  }


}
