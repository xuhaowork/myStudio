package org.apache.spark.mllib.classification.polr

object ArrayUtilsImplicit {
  implicit class ArrayCumsum(values: Array[Double]){
    def cumsum: Array[Double] = values
      .foldLeft(Array.empty[Double])((arr, v) => arr :+ (if(arr.isEmpty) v else arr.last + v))
  }

}
