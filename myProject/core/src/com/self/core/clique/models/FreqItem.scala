package com.self.core.clique.models

import scala.collection.mutable.ArrayBuffer

/**
  * 树形频次统计的基本结点
  * ----
  * 应用于维度之间有树形下钻关系的频次统计, 如[(国家, 省份, 城市), 频次]:
  * (0, 0, 0), 14L
  * (0, 0, 1), 12L
  * ...
  * (0, 1, 0), 1L
  * (2, 0, 1), 2L
  * 需要分别统计, 国家的频次, 国家 + 省份的频次, 国家 + 省份 + 城市的频次
  *
  * @param key       当前统计对应的key
  * @param frequency 频次
  * @param children  维度在该[[key]]上下钻到下一个维度的频次统计结果
  */
case class FreqItem(key: Int, frequency: Long, children: ArrayBuffer[FreqItem]) {


}
