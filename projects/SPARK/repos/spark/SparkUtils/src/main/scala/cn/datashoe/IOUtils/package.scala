package cn.datashoe

import java.io.File
import java.nio.charset.Charset

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog


package object IOUtils {

  /** mongodb读取 */
  object MongodbIO {

    import cn.datashoe.numericToolkit.Toolkit
    import com.mongodb.spark.MongoSpark
    import com.mongodb.spark.config._
    import com.mongodb.spark.sql._
    import org.apache.spark.SparkContext
    import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.{DataFrame, SparkSession}
    import org.bson
    import org.bson.BsonDocument

    var conf: Map[String, String] = Map("port" -> "27017")

    def setHost(host: String): this.type = {
      conf += "host" -> host
      this
    }

    def setPort(port: String): this.type = {
      conf += "port" -> port
      this
    }

    def setDatabase(database: String): this.type = {
      conf += "database" -> database
      this
    }

    def setTable(table: String): this.type = {
      conf += "table" -> table
      this
    }

    def read(sc: SparkContext): DataFrame = {
      val sqlContext = SparkSession.builder.getOrCreate().sqlContext
      val mongodbUrl = "mongodb://" + conf("host") + ":" + conf("port") + "/" + conf("database") + "." + conf("table")

      sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> mongodbUrl /*, "partitioner" -> "MongoShardedPartitioner"*/)))
    }

    /** 分区读取 */
    def readByPartition(sc: SparkContext): DataFrame = {
      /** 字段 */
      val fieldTem = Array("")
      val fieldType = Array("")
      val schemaSeq = for ((name, dataType) <- fieldTem.zip(fieldType)) yield {
        StructField(name, CatalystSqlParser.parseDataType(dataType), true)
      }

      val schema = StructType(schemaSeq)

      val partitionType = ""

      //filter  cause
      val query = ""
      //  define function  field  match  data type
      val column = ""
      val mongodbUrl = "mongodb://" + conf("host") + ":" + conf("port") + "/" + conf("database") + "." + conf("table")
      val rdd = partitionType match {
        case "MongoDefaultPartitioner" =>
          // 分区键
          val partitionKey = ""
          val partitionSizeMB = "64 MB"
          Toolkit.toRangeType[Long](partitionSizeMB, 0, Long.MaxValue, rightExOrInClusive = true, msgInfo = s"输入的分区抽样大小应在(0,${Long.MaxValue}]")
          // 每个分区抽样数
          val samplesPerPartition = "10"
          // 数据验证
          Toolkit.toRangeType[Int](samplesPerPartition, 0, Integer.MAX_VALUE, rightExOrInClusive = true, msgInfo = s"输入的分区抽样大小应在(0,${Integer.MAX_VALUE}]")
          MongoSpark.load[BsonDocument](sc, ReadConfig(Map("uri" -> mongodbUrl, "partitioner" -> "DefaultMongoPartitioner",
            "partitionKey" -> partitionKey, "partitionSizeMB" -> partitionSizeMB, "samplesPerPartition" -> samplesPerPartition)))

        case "MongoShardedPartitioner" =>
          val partitionKey = "" // 分区键
          MongoSpark.load(sc, ReadConfig(Map("uri" -> mongodbUrl, "spark.mongodb.input.partitioner" -> "MongoShardedPartitioner",
            "shardKey" -> partitionKey)))
        case "MongoSplitVectorPartitioner" =>
          // 分区键
          val partitionKey = ""
          val partitionSizeMB = "64 MB"
          // 数据验证
          Toolkit.toRangeType[Long](partitionSizeMB, 0, Long.MaxValue, rightExOrInClusive = true, msgInfo = s"输入的分区抽样大小应在(0,${Long.MaxValue}]")
          MongoSpark.load(sc, ReadConfig(Map("uri" -> mongodbUrl, "partitioner" -> "MongoSplitVectorPartitioner",
            "partitionKey" -> partitionKey, "partitionSizeMB" -> partitionSizeMB)))
        case "MongoPaginateByCountPartitioner" =>
          // 分区键
          val partitionKey = ""
          // 分区数
          val numberOfPartitions = "10"
          Toolkit.toRangeType[Int](numberOfPartitions, 0, Integer.MAX_VALUE, rightExOrInClusive = true, msgInfo = s"输入的分区抽样大小应在(0,${Integer.MAX_VALUE}]")
          MongoSpark.load(sc, ReadConfig(Map("uri" -> mongodbUrl, "partitioner" -> "MongoPaginateByCountPartitioner",
            "partitionKey" -> partitionKey, "numberOfPartitions" -> numberOfPartitions)))

        case "MongoPaginateBySizePartitioner" =>
          val partitionKey = ""
          val partitionSizeMB = "64 MB"
          // 数据验证
          Toolkit.toRangeType[Long](partitionSizeMB, 0, Long.MaxValue, rightExOrInClusive = true, msgInfo = s"输入的分区抽样大小应在(0,${Long.MaxValue}]")
          MongoSpark.load(sc, ReadConfig(Map("uri" -> mongodbUrl, "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
            "partitionKey" -> partitionKey, "partitionSizeMB" -> partitionSizeMB)))
      }

      var outDf: DataFrame = null

      // 构造Projection
      val projection = new BsonDocument("$project", BsonDocument.parse(column))
      if (query.isEmpty) {
        val aggregatedRDD = rdd.withPipeline(Seq(projection))
        if (aggregatedRDD.isEmpty())
          outDf = aggregatedRDD.toDF(schema)
        else
          outDf = aggregatedRDD.toDF()
        outDf
      } else {
        val queryMongodb = new bson.Document("$match", BsonDocument.parse(query))
        //val matchQuery = new Document("$match", BsonDocument.parse("{\"type\":\"1\"}"))
        val aggregatedRDD = rdd.withPipeline(Seq(queryMongodb, projection))
        //  val aggregatedRDD = rdd.withPipeline(Seq(projection1))

        if (aggregatedRDD.isEmpty())
          outDf = aggregatedRDD.toDF(schema)
        else
          outDf = aggregatedRDD.toDF()
        outDf
      }
    }

  }


  object JDBC {

    var conf: Map[String, String] = Map("port" -> "27017")

    def setHost(host: String): this.type = {
      conf += "host" -> host
      this
    }

    def setPort(port: String): this.type = {
      conf += "port" -> port
      this
    }

    def setDatabase(database: String): this.type = {
      conf += "database" -> database
      this
    }

    def setTable(table: String): this.type = {
      conf += "table" -> table
      this
    }


    def read(): Unit = {
      import java.util.Properties

      import org.apache.spark.sql.SQLContext

      val prop = new Properties()
      prop.setProperty("user", "")
      prop.setProperty("password", "")

      prop.setProperty("zeroDateTimeBehavior", "convertToNull") //非法日期值转为null

      prop.setProperty("driver", "com.mysql.jdbc.Driver")
      prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
      prop.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      prop.setProperty("driver", "com.gbase.jdbc.Driver")
      prop.setProperty("driver", "org.postgresql.Driver")

      val sql = "select * from user"

      val url = "jdbc:mysql://mysqlHost:3306/database"
      val sQLContext: SQLContext = null

      // 还可以添加predicates: Array("2016-01-02 00:00:00", "2016-01-02 01:00:00")
      val comm = sQLContext.read.jdbc(url, "(" + sql + ") TEMPTABLEQQQ", prop)
      val df = comm.repartition(5)
      df.cache()


      // mpp
      {
        import java.util.Properties

        val rddTableName = "<#rddtablename#>"

        val sql = "(select * from tl_testdata_filter) as aa"
        val prop = new Properties()
        prop.setProperty("user", "bgetl")
        prop.setProperty("password", "Bigdata123@")
        prop.setProperty("driver", "org.postgresql.Driver")
        val url = "jdbc:postgresql://%s:%d/%s".format("11.39.222.98", 25308, "ods")
        val newDataFrame = sqlc.read.jdbc(url, sql, prop)

        newDataFrame.cache()
        outputrdd.put(rddTableName, newDataFrame)
        newDataFrame.registerTempTable(rddTableName)
        sqlc.cacheTable(rddTableName)

        newDataFrame.show()

      }


      // jdbc
      def readDataBase(url: String = "jdbc:mysql://192.168.11.26:3306/test",
                       table: String,
                       usr: String = "oozie",   // "oozie",
                       passWord: String = "oozie",
                       dataBaseType: String = "MySQL"): DataFrame = {
        val prop = new Properties()
        prop.setProperty("user", usr)
        prop.setProperty("password", passWord)
        dataBaseType.toLowerCase match {
          case "mysql" => prop.setProperty("driver", "com.mysql.jdbc.Driver")
          case "oracle" => prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
          case "sqlserver" => prop.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
          case "gbase" => prop.setProperty("driver", "com.gbase.jdbc.Driver")
        }
        sqlc.read.jdbc(url, table, prop)
      }

    }


  }


  object HBase {
    def read(): Unit = {
      val tableName = "table"
      val family = ""
      val column = ""
      val sQLContext: SQLContext = null
      val catalogTemplate =
        s"""{
           |"table":{"namespace":"default", "name":"$tableName", "tableCoder":"PrimitiveType"},
           |"rowkey":"key",
           |"columns":{
           |"RowKey":{"cf":"rowkey", "col":"key", "type":"string"},
           |"id":{"cf":"$family", "col":"$column", "type":"string"}
           |}
           |}""".stripMargin

      val hbaseConfig: Configuration = HBaseConfiguration.create()

      val df = sQLContext.read
        .options(Map(HBaseTableCatalog.tableCatalog -> catalogTemplate))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
      df

    }

  }


  // 文件读取
  object fileIO {
    def read(): Unit = {
      FileUtils.readFileToString(new File(pth), Charset.forName(encoding))
    }

    /**
      * 为路径加上user.dir
      * ----
      * 功能: 用于一些resources文件读取, 确保在不同机器运行效果一致
      * ----
      *
      * @param path 路径
      * @return 绝对路径
      */
    def addUserDir2Path(path: String): String = {
      println(s"user.dir为: ${System.getProperty("user.dir")}")
      val userDir = s"${System.getProperty("user.dir")}".replaceAll("\\\\", "\\/")
      if (userDir.endsWith("/") || path.replaceAll("\\\\", "\\/").endsWith("/"))
        userDir + path.replaceAll("\\\\", "\\/")
      else
        userDir + "/" + path.replaceAll("\\\\", "\\/")
    }

  }


}
