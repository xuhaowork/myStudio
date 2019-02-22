package cn.datashoe.utils

import java.io.{File, FileOutputStream}

package object IOUtils {

  /**
    * 示例: 按行读取
    */
  object ReadByLine {

    import java.io.FileInputStream

    def main(args: Array[String]): Unit = {
      import scala.io.Source
      val ss = Source.fromFile("F:\\My_Workplace\\data\\LANL-Earthquake-Prediction\\train.csv").getLines()
      for(each <- ss) {
        println(each)
      }

    }





  }


}
