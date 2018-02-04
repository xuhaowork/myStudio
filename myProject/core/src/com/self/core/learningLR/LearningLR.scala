package com.self.core.learningLR

import com.self.core.baseApp.myAPP

object LearningLR extends myAPP{
  override def run(): Unit = {
    import org.apache.spark.mllib.classification.LogisticRegressionWithSGD


/**
    * where \delta_{i, j} = 1 if i == j,
    *       \delta_{i, j} = 0 if i != j, and
    *       multiplier =
      *         \exp(margins_i) / (1 + \sum_k^{K-1} \exp(margins_i)) - (1-\alpha(y)\delta_{y, i+1})

*/











  }
}
