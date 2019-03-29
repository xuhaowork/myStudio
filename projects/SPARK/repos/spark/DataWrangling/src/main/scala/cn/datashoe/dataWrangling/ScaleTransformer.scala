package cn.datashoe.dataWrangling

/**
  * 尺度转化
  * ----
  * 数据的尺度转化工具类
  * ----
  * 1)z-score
  * 2)min-max
  * 3)log
  * 4)Box-Cox
  * 5)sigmoid
  * 6)atan
  */
object ScaleTransformer extends Serializable {
  /**
    * z-score变换
    *
    * @param value 数据
    * @param mean  均值
    * @param std   方差 方差大于等于0.0, 当等于0.0时所有数据一样, 此时变换直接返回0.0
    * @return
    */
  def z_score(value: Double, mean: Double, std: Double): Double =
    if (std < 0.0)
      throw new IllegalArgumentException(s"您输入的标准差'$std'小于0, 不符合标准差的定义")
    else if (std == 0.0)
      value - mean
    else
      (value - mean) / std

  /**
    *
    * @param value     数据
    * @param min       最小值
    * @param deviation 离差 离差大于等于0.0, 当等于0.0时所有数据一样, 此时变换直接返回0.0
    * @return
    */
  def min_max(value: Double, min: Double, deviation: Double): Double =
    if (deviation < 0.0)
      throw new IllegalArgumentException(s"您输入的离差(最大值最小值之差)为'$deviation', 最大值比最小值小")
    else if (deviation == 0.0)
      0.0
    else
      (value - min) / deviation

  def box_cox(value: Double, lambda: Double): Double = {

    100.0
  }



}
