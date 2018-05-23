package org.apache.spark.mllib.binary

import com.zzjz.deepinsight.basic.BaseMain
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.binary.load.{BinaryFiles, Conversion}

/**
  *   author: Bai Yuan
  *   date: 2018/1/18
  * */

object BinaryOverlap extends BaseMain{
    override  def  run:Unit={

      val path="D:\\binarytest\\baiyuan.txt"
      val conf = new Configuration()
      val fs= FileSystem.get(conf)
      val file=new Path(path)

      if(!fs.exists(file)) throw new Exception(String.format("文件路径%s不存在",file))
      val  status=fs.listStatus(file)
      if (status.length == 0) throw new Exception("文件夹下没有文件")

      import sqlc.implicits._
      val rdd = new BinaryFiles(sc).binaryFiles("D:\\binarytest\\baiyuan.txt",0,6)

      rdd.collect().foreach(x=> println(Conversion.bytes2hex(x._2.toArray())))
      //正规程序
      val   rdd1=rdd.map(x=>(x._1,x._2.getOffset(),x._2.getLength(),x._2.toArray()))

      val   df1= rdd1.toDF("fileName","offset","length","arrayByte")
       df1.show


      val  tem=df1.rdd.map(x=>new String(x.getAs[Array[Byte]]("arrayByte")))
      tem.saveAsTextFile("D:\\baibai")


      /*val  tem=df1.rdd.map(x=>new String(x.getAs[Array[Byte]]("arrayByte")))
      tem.saveAsTextFile("D:\\baiyuan")*/
      //.map(x=>x.mkString("")).collect()
      //rdd2.saveAsTextFile("D:\\baiyuan")
      // rdd.collect.foreach(x=> println(Conversion.bytes2hex02(x._2.toArray()).slice(0,20)))
      // rdd.map(p => Hex.encodeHex(p._2.toArray())).flatMap(r=>r).collect().slice(0,2000).foreach(print _)
      // println(Conversion.bytes2hex02(rdd.flatMap(r=>r._2.toArray()).collect()))
      //  rdd.collect().foreach(x=>x.2_)


  }
}
