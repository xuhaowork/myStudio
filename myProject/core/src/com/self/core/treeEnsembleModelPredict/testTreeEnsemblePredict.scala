package com.self.core.treeEnsembleModelPredict

import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.tree.model.Node
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object testTreeEnsemblePredict extends myAPP {
  override def run(): Unit = {
    println("good")

    import org.apache.spark.mllib.linalg.Vectors
    val data1 = Array(
      (0.0, Vectors.dense(Array(32, 1, 1, 0).map(_.toDouble))),
      (0.0, Vectors.dense(Array(25, 1, 2, 0).map(_.toDouble))),
      (3.0, Vectors.dense(Array(29, 1, 2, 1).map(_.toDouble))),
      (1.0, Vectors.dense(Array(24, 1, 1, 0).map(_.toDouble))),
      (3.0, Vectors.dense(Array(31, 1, 1, 0).map(_.toDouble))),
      (2.0, Vectors.dense(Array(35, 1, 2, 1).map(_.toDouble))),
      (0.0, Vectors.dense(Array(30, 0, 1, 0).map(_.toDouble))),
      (3.0, Vectors.dense(Array(31, 1, 1, 0).map(_.toDouble))),
      (1.0, Vectors.dense(Array(30, 1, 2, 1).map(_.toDouble))),
      (2.0, Vectors.dense(Array(21, 1, 1, 0).map(_.toDouble))),
      (0.0, Vectors.dense(Array(21, 1, 2, 0).map(_.toDouble))),
      (1.0, Vectors.dense(Array(21, 1, 2, 1).map(_.toDouble))),
      (3.0, Vectors.dense(Array(29, 0, 2, 1).map(_.toDouble))),
      (0.0, Vectors.dense(Array(29, 1, 0, 1).map(_.toDouble))),
      (2.0, Vectors.dense(Array(29, 0, 2, 1).map(_.toDouble))),
      (1.0, Vectors.dense(Array(30, 1, 1, 0).map(_.toDouble)))
    )

    val rawDataFrame1 = sqlc.createDataFrame(data1).toDF("label", "feature")

    val data2 = Array(
      (0.0, Vectors.dense(Array(32, 1, 2, 0).map(_.toDouble))),
      (1.0, Vectors.dense(Array(27, 0, 1, 1).map(_.toDouble))),
      (1.0, Vectors.dense(Array(22, 0, 1, 0).map(_.toDouble))),
      (1.0, Vectors.dense(Array(25, 1, 1, 1).map(_.toDouble))),
      (0.0, Vectors.dense(Array(23, 0, 2, 1).map(_.toDouble)))
    )
    val rawDataFrame2 = sqlc.createDataFrame(data2).toDF("label", "feature")


    rawDataFrame1.show()
    rawDataFrame2.show()


    def transform(rawDataFrame1: DataFrame): RDD[LabeledPoint] = {
      rawDataFrame1.select("label", "feature").mapPartitions(rowPartition =>
        rowPartition.filter(row => !row.isNullAt(0) && !row.isNullAt(1)).map(row => {
          val label = row.get(0) match {
            case d: Double => d
            case i: Int => i.toDouble
          }
          val feature = row.get(1) match {
            case dv: DenseVector => dv
            case sv: SparseVector => sv.toDense
          }

          LabeledPoint(label, feature)
        })
      )
    }


    val trainData: RDD[LabeledPoint] = transform(rawDataFrame1)
    val testData: RDD[LabeledPoint] = transform(rawDataFrame2)


    //分类
    val numClasses = 4
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"

    //最大深度
    val maxDepth = 5
    //最大分支
    val maxBins = 32

    //模型训练
    val model: DecisionTreeModel = DecisionTree.trainClassifier(trainData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    import org.apache.spark.mllib.tree.model.DecisionTreeModelImpl


    //模型预测
    testData.map { point =>
      val prob = model.predictProb(point.features)
      val prediction = model.predict(point.features)
      (point.label, prediction, prob)
    }.foreach(println)

//    val someV = Vectors.dense(Array(25.0, 1.0, 2.0, 1.0))
//
//
//    def foreachPrint(node: Node): Unit = {
//      if(node.isLeaf){
//        println(node.predict.toString())
//      }else{
//        if(node.leftNode.isDefined){
//          println(node.leftNode.get.toString())
//          foreachPrint(node.leftNode.get)
//        }else{
//          println(node.rightNode.get.toString())
//          foreachPrint(node.rightNode.get)
//        }
//
//      }
//
//
//    }
//
//    foreachPrint(model.topNode)
//    println("*" * 80)
//
//    foreachPrint(model.topNode.toNodeWithProbPredict)
//
//    println(model.topNode.toNodeWithProbPredict.predictProb(someV))
//    println(model.predictProb(someV))
//    println(model.topNode.leftNode)
//    println(model.topNode.rightNode)


  }
}
