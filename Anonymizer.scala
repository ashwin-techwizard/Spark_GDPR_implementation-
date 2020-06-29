package ashwin.gdpr.transform

import java.text.SimpleDateFormat

import ashwin.gdpr.anonymizer.HDFSAnonymizeDriver.logger
import ashwin.gdpr.config.Constants
import ashwin.gdpr.entity.{DpoErasureLog, DpoGenomeObjectMapping, MetaData}
import ashwin.gdpr.transform.ErasureLogProcessor._
import ashwin.gdpr.utils.ExceptionModule.logException
import ashwin.gdpr.utils.HBaseConnection.getTable
import ashwin.gdpr.utils.HDFSUtils._
import org.apache.hadoop.hbase.client.Table
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.MutableList

object HDFSAnonymiser {


  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName("Rapid")
      //.master("local")
      .getOrCreate()


    //TODO: get configuration from hbase config table and loop throw file types



    val piiFields = List(("PAXNAM", "XXXXXX")) // val piiFields = composePIIList(piFields, patternMap, rowKeyColumn) //List(("CMT_TXT", "XXXXXX"), ("STAFF_NO",0))
    val filePath=args(0)
    val rowKey=args(1)
    val keyColumn=args(2)
     val outputPath=args(3)
    val metaMap = scanHDFSforLineage(sparkSession, filePath, rowKey.split(","), keyColumn, 1)
    println("metaMap>>>" + metaMap)

    //val rowKeyColumn = args(3)
    doAnonimize(sparkSession,outputPath,metaMap,piiFields,keyColumn)

    //TODO: Update status to log table

  }

  private def doAnonimize(sparkSession: SparkSession,tempPath:String,metaMap: Map[String, mutable.Set[String]],piiFields :List[(String,String)],rowKeyColumn:String) = {
    metaMap.par.foreach {
      fileData =>
        val hdfsFilePath = fileData._1
        val outputPath = tempPath + (hdfsFilePath.replaceAll("hdfs://", "").replaceAll("/", "_"))

        val dfFile = sparkSession.read.format("com.databricks.spark.avro")
          .load(hdfsFilePath)
        val outDF = piiFields.foldLeft(dfFile)((df, fieldPattern) =>
          df.withColumn(fieldPattern._1,
            when(col(rowKeyColumn).isin(fileData._2.toList: _ *) && col(rowKeyColumn).isin(fileData._2.toList: _ *) ,
              lit(fieldPattern._2)).otherwise(col(fieldPattern._1))))
        outDF.write.mode("overwrite")
          .format("com.databricks.spark.avro").save(outputPath)
    }
  }

  def scanHDFSforLineage(sparkSession: SparkSession, inputFolderPath: String, filterKeyList: Array[String], filterColumn: String, pathLevel: Int): scala.collection.immutable.Map[String, scala.collection.mutable.Set[String]] = {
    val df = sparkSession.read.format("com.databricks.spark.avro").load(inputFolderPath)
    val lineage = df.filter(col(filterColumn).isin(filterKeyList: _*)).select(input_file_name() alias ("folder"), col(filterColumn) alias ("key")).rdd.map { row =>

      var path = row.getAs[String]("folder")
      var level = pathLevel
      while (level > 0) {
        path = path.substring(0, path.lastIndexOf("/"))
        level -= 1
      }
      (path, scala.collection.mutable.Set(row.getAs[String]("key")))
    }.reduceByKey(_ ++ _).repartition(1).cache()

    println("count>>?> " + lineage.count())
    lineage.foreach(println)
    println("MAP>>" + lineage.collectAsMap().toString())
    val metaMap = lineage.collectAsMap()
    metaMap.toMap //[String,scala.collection.immutable.Set[String]]
  }


  def updateStatus(sparkSession: SparkSession, GENOME_PREFIX: String, DPO_ERASURE_LOG: String,
                             objectMapping: DpoGenomeObjectMapping,
                             tableConfig: MetaData,
                             FOLDER_META_PATH: String,
                             FOLDER_TMP_PATH: String
                            ): Unit = {

    logger.info("[INFO] DsarErasureHDFS.dsarErasureHDFSfile is invoked")

    val prefixIndicator: String = objectMapping.prefixIndicator.getOrElse(null)
    val hdfsBasePath: String = objectMapping.hdfsPath.getOrElse(null)
    val piFields: Set[String] = tableConfig.tablePII.toSet
    val patternMap: Map[String, String] = tableConfig.patternsMap.asScala.toMap
    val indexMap: Map[String, String] = tableConfig.indexMap.asScala.toMap

    val rowKeyColumn = indexMap.get("rowKeyColumn").get

    // Getting the Processing Time Stamp
    val processingTimesStamp = System.currentTimeMillis()

    val erasureLogTable: Table = getTable(DPO_ERASURE_LOG)

    val erasureLogRecords: Map[String, DpoErasureLog] = null//DPOTablesProcessor.getErasureLogRecords(erasureLogTable, prefixIndicator)
println("erasureLogRecords>>"+erasureLogRecords)
    val entireRowKeyList = MutableList[String]()

    if (erasureLogRecords != null && erasureLogRecords.size > 0) {
      erasureLogRecords.foreach { erasureLogRecord =>
        val rowKeyList = erasureLogRecord._2.staffNo.getOrElse(Constants.EMPTY).split(Constants.COMMA).toList
        if (rowKeyList != null)
          rowKeyList.foreach(rowKey =>
            entireRowKeyList += rowKey)
      }
println("entireRowKeyList>>>"+entireRowKeyList)
      var lineageMapSuccess = mutable.Map[String, MutableList[String]]()
      var lineageMapRejected = mutable.Map[String, MutableList[(String, String)]]()

      val metaMap = scanHDFSforLineage(sparkSession, hdfsBasePath, entireRowKeyList.toArray, rowKeyColumn, 1)
      println("metaMap>>>" + metaMap)
      val piiFields = composePIIList(piFields, patternMap, rowKeyColumn) //List(("CMT_TXT", "XXXXXX"), ("STAFF_NO",0))
      var folderList = MutableList[(String, String)]()
      val hBaseEntirMap = mutable.Map[String,mutable.Map[String, String]]()
      metaMap.par.foreach {
        fileData =>
          val hdfsFilePath = fileData._1
          logger.info("[INFO] Source Path is:" + hdfsFilePath)
          if (validateHDFSDirectory(sparkSession, hdfsFilePath)) {
            val outputPath = FOLDER_TMP_PATH + Constants.SLASH + prefixIndicator + Constants.SLASH + (hdfsFilePath.replaceAll("hdfs://", "").replaceAll("/", "_"))
            logger.info("[INFO] OutputPath is:" + outputPath)
            try {

              val dfFile = sparkSession.read.format("com.databricks.spark.avro").load(hdfsFilePath)
              val outDF = piiFields.foldLeft(dfFile)((df, fieldPattern) => df.withColumn(fieldPattern._1, when(col(rowKeyColumn).isin(fileData._2.toList: _ *), lit(fieldPattern._2)).otherwise(col(fieldPattern._1))))
              outDF.write.mode("overwrite").format("com.databricks.spark.avro").save(outputPath)

              lineageMapSuccess = getLineageMapSuccess(lineageMapSuccess, fileData._2, hdfsFilePath, indexMap)
              val folderTuple = (hdfsFilePath, outputPath)
              folderList += folderTuple
            } catch {
              case x: Exception => {
                val hBaseValueMap = mutable.Map[String, String]()
                hBaseValueMap += ("exception" -> "Anonymize Failed")
                hBaseValueMap += ("exceptionLog" -> x.getStackTrace.toString)
                hBaseValueMap += ("rowKeyList" -> fileData._2.toString())
                hBaseEntirMap+=("CREW"+prefixIndicator+fileData._2-> hBaseValueMap)

                logger.info("[INFO] Exception: Anonymize Failed " + prefixIndicator + " Type" + x.printStackTrace())
                lineageMapRejected = updateFailure(lineageMapRejected, fileData, hdfsFilePath, "[ERROR] Anonymize failed for:" +
                  prefixIndicator + " [REASON] " + x.getMessage, indexMap)
              }
            }
          } else {
            lineageMapRejected = updateFailure(lineageMapRejected, fileData, hdfsFilePath, "[ERROR] Invalid Source Directory Path", indexMap)
          }
      }
      logException(hBaseEntirMap)
      import sparkSession.implicits._

      //TODO : Write the data only when there is success lineage map found
      val folderDf = folderList.toDF("input", "output")

      folderDf.repartition(1).write.mode("append").format("csv").option("header", "false").save(FOLDER_META_PATH)

      val erasureLogStatus = buildErasureLogStatusStaffId(erasureLogRecords, indexMap, lineageMapRejected, lineageMapSuccess, processingTimesStamp)

      updateErasureLogStatus(erasureLogTable, erasureLogStatus)
    }
  }

  def buildErasureLogStatusStaffId(erasureLogRecords: Map[String, DpoErasureLog], indexMap: Map[String, String], lineageMapRejected: mutable.Map[String, MutableList[(String, String)]],
                                   lineageMapSuccess: mutable.Map[String, MutableList[String]], processingTimesStamp: Long): mutable.Map[String, mutable.Map[String, String]] = {
    //TODO Pass the Lineage row keys
    logger.info("[INFO] DsarErasureHDFS.updateErasureLogStatus is invoked")
    var erasureLogStatusMap = mutable.Map[String, mutable.Map[String, String]]()
    val hBaseExceptionEntirMap = mutable.Map[String,mutable.Map[String, String]]()
    val hBaseExceptionValue = mutable.Map[String, String]()
    erasureLogRecords.foreach { dsarCustom =>
      val logTableRowKey = dsarCustom._1
      val rowKeyList = dsarCustom._2.staffNo.getOrElse(Constants.EMPTY).split(Constants.COMMA)
      if (rowKeyList != null) {
        var SUCCESS = false
        var FAILED = false
        val hBaseValueMap = mutable.Map[String, String]()
        var logBuilder = new StringBuilder()

        rowKeyList.foreach {
          rowKey =>
            if (lineageMapRejected.contains(rowKey)) {
              FAILED = true
              logBuilder ++= rowKey + " FAILED - " + lineageMapRejected.get(rowKey).get.toString().replaceAll("MutableList", "") + " \n"
            } else if (lineageMapSuccess.contains(rowKey)) {
              SUCCESS = true
              logBuilder ++= rowKey + " SUCCESS - " + lineageMapSuccess.get(rowKey).get.toString().replaceAll("MutableList", "") + " \n"
            } else {
              SUCCESS = true
              logBuilder ++= rowKey + " FAILED - RECORD NOT FOUND \n"
            }
        }
        if (FAILED) {
          hBaseValueMap += ("status" -> "FAILED")
          hBaseExceptionValue += ("exception" -> "Anonymize Failed")
          hBaseExceptionValue += ("exceptionLog" -> logBuilder.toString())
          hBaseExceptionEntirMap+=(logTableRowKey-> hBaseExceptionValue)

        } else if (SUCCESS) {
          hBaseValueMap += ("status" -> "SUCCESS")
        }
        hBaseValueMap += ("log" -> logBuilder.toString())
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
        hBaseValueMap += ("processTime" -> dateFormat.format(processingTimesStamp))
        logBuilder.clear()
        erasureLogStatusMap += (logTableRowKey -> hBaseValueMap)
      }
    }
    logger.info("[INFO] Final Erasure Log Status is:" + erasureLogStatusMap)

    logException(hBaseExceptionEntirMap)

    erasureLogStatusMap
  }
  def composePIIList(selectiveAnonyPath: Set[String], anonymPatterns: Map[String, Any], colKey: String): List[(String, Any)] = {
    // We work with a set to ensure there are no duplicates, in case conf file was poorly defined
    val piiMap = scala.collection.mutable.Set[(String, Any)]()
    selectiveAnonyPath.map { path =>
      val attName = if (path.contains('.'))
        path.substring(0, path.indexOf("."))
      else if (path.contains("###"))
        path.substring(0, path.indexOf("#"))
      else {
        val errrMsg = "Exception, path doesnt contain ### so it follows wrong format " + path
        //TODO: Capture exception
        return null
        ""
      }
      val attType = path.substring(path.lastIndexOf("#") + 1)
      piiMap.add((attName, anonymPatterns(attType)))
    }
    // To ensure the keyColum goes last
    piiMap.toList.sortWith((tup1, tup2) => tup1._1 != colKey)
  }

}