package org.apache.spark.sql.dbPartiiton

import com.google.gson.JsonParser
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.self.core.baseApp.myAPP
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.dbPartiiton.partition.Analysis
import org.bson
import org.bson.BsonDocument

/**
  *   author: data
  *   date:   2018/1/19
* */

object MongoDB  extends  myAPP{

     override  def  run:Unit={

       val jsonparam =
         """  {"DBType":{"value":"mongodb"},"PartitionType":{"value":"MongoDefaultPartitioner","PartitionKey":"_id","PartitionSizeMB":"64 MB",
           |"SamplesPerPartition":"10"},
           |"host":"192.168.11.110","port":"10005","ConnType":"Host","Base":"json","Table":"METADATA_SatDowDemDec",
           |"Field":[{"displayName":"CNR(Double)","name":"CNR","datatype":"Double"},
           |{"displayName":"CRCCodGenPol(String)","name":"CRCCodGenPol","datatype":"String"},
           |{"displayName":"EBEMFECParII(String)","name":"EBEMFECParII","datatype":"String"}],
           |"Filter":{"displayName":"ObjID>1,captureTime>5,carTraInsNum<6,ts>"20180122"",
           |"allData":["ObjID(String)","antennaDirection(String)","captureTime(String)","carTraInsNum(Integer)","ts(Date)"]}}""".stripMargin

       val parser = new JsonParser()
       val temJsonParser=parser.parse(jsonparam).getAsJsonObject
       val host=temJsonParser.get("host").getAsString
       val port=temJsonParser.get("port").getAsString
       val base=temJsonParser.get("Base").getAsString
       val table=temJsonParser.get("Table").getAsString
       val fieldTem=temJsonParser.getAsJsonArray("Field")
       val partiitionType=temJsonParser.getAsJsonObject("PartitionType")

       val filterTem=temJsonParser.getAsJsonObject("Filter")
       val filter=filterTem.get("displayName").getAsString
       val dataType=filterTem.getAsJsonArray("allData")

       val column=Analysis.mongoSelect(fieldTem)
       val mongodbUrl = "mongodb://" + host + ":" + port +"/" + base +"." + table
      // var rdd=MongoSpark.load(sc,ReadConfig(Map("uri" -> mongodbUrl, "partitioner" -> "MongoShardedPartitioner")))
      /* val df = sqlContext.read.format("com.mongodb.spark.sql").options(
         Map("spark.mongodb.input.uri" -> mongodbUrl,
           "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
           "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
           "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32"
         ))
         .load()*/

     val   rdd= partiitionType.get("value").getAsString match {
           case  "MongoDefaultPartitioner" =>
             val  partitionKey=partiitionType.get("PartitionKey").getAsString
             val  partitionSizeMB=partiitionType.get("PartitionSizeMB").getAsString.toUpperCase.replaceAll(" ","").replaceAll("MB","")
             val  samplesPerPartition=partiitionType.get("SamplesPerPartition").getAsString
             MongoSpark.load(sc,ReadConfig(Map("uri" -> mongodbUrl, "partitioner" -> "DefaultMongoPartitioner",
             "partitionKey"->partitionKey,"partitionSizeMB"->partitionSizeMB,"samplesPerPartition"->samplesPerPartition)))

           case "MongoShardedPartitioner"=>
             val shardkey=partiitionType.get("Shardkey").getAsString
             MongoSpark.load(sc,ReadConfig(Map("uri" -> mongodbUrl, "partitioner" -> "MongoShardedPartitioner",
               "shardKey"->shardkey)))
           case "MongoSplitVectorPartitioner"=>
             val  partitionKey=partiitionType.get("PartitionKey").getAsString
             val  partitionSizeMB=partiitionType.get("PartitionSizeMB").getAsString.toUpperCase.replaceAll(" ","").replaceAll("MB","")
             MongoSpark.load(sc,ReadConfig(Map("uri" -> mongodbUrl, "partitioner" -> "MongoSplitVectorPartitioner",
               "partitionKey"->partitionKey,"partitionSizeMB"->partitionSizeMB)))
           case "MongoPaginateByCountPartitioner"=>
             val  partitionKey=partiitionType.get("PartitionKey").getAsString
             val  numberOfPartitions=partiitionType.get("NumberOfPartitions").getAsString.toUpperCase.replaceAll(" ","")
             MongoSpark.load(sc,ReadConfig(Map("uri" -> mongodbUrl, "partitioner" -> "MongoPaginateByCountPartitioner",
               "partitionKey"->partitionKey,"numberOfPartitions"->numberOfPartitions)))
           case "MongoPaginateBySizePartitioner"=>
             val  partitionKey=partiitionType.get("PartitionKey").getAsString
             val  partitionSizeMB=partiitionType.get("PartitionSizeMB").getAsString.toUpperCase.replaceAll(" ","").replaceAll("MB","")
             MongoSpark.load(sc,ReadConfig(Map("uri" -> mongodbUrl, "partitioner" ->"MongoPaginateBySizePartitioner",
             "partitionKey"->partitionKey,"partitionSizeMB"->partitionSizeMB)))
     }
       var  outDf:DataFrame=null
       // 构造Projection
       val projection = new BsonDocument("$project", BsonDocument.parse(column))
       if (filter.length==0){
         val aggregatedRDD = rdd.withPipeline(Seq(projection))
         outDf=aggregatedRDD.toDF()
       }else {
         val query = Analysis.monsqlCov(filter, dataType)
         val queryMongo = new bson.Document("$match", BsonDocument.parse(query))
         //val matchQuery = new Document("$match", BsonDocument.parse("{\"type\":\"1\"}"))
         val aggregatedRDD = rdd.withPipeline(Seq(queryMongo, projection))
         //  val aggregatedRDD = rdd.withPipeline(Seq(projection1))
         outDf=aggregatedRDD.toDF()
       }

       outputrdd.put("<#rddtablename#>",outDf)
       outDf.registerTempTable("<#rddtablename#>")
       sqlc.cacheTable("<#rddtablename#>")
     }
}
