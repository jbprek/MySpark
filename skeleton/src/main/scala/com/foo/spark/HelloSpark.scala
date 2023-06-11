package com.foo.spark

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.io.Source
import scala.language.implicitConversions

object HelloSpark extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {




    if (args.length == 0) {
      logger.error("Usage: HelloSpark filename")
      System.exit(1)
    }

    logger.info("Starting Hello Spark")
    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()
    //logger.info("spark.conf=" + spark.conf.getAll.toString())


    import spark.implicits._
    val df1 = Seq(1, 2).toDF

    case class A(id: Int)

    var a: Dataset[A] = null

    val surveyRawDF = loadSurveyDF(spark, args(0))
    val partitionedSurveyDF = surveyRawDF.repartition(2)
    val countDF = countByCountry(partitionedSurveyDF)
    countDF.foreach(row => {
      logger.info("Country: " + row.getString(0) + " Count: " + row.getLong(1))
    })

    logger.info(countDF.collect().mkString("->"))

    logger.info("Finished Hello Spark")
    //scala.io.StdIn.readLine()
    spark.stop()
  }

  def countByCountry(surveyDF: DataFrame): DataFrame = {
    surveyDF.where("Age < 40")
      .select("Age", "Gender", "Country", "state")
      .groupBy("Country")
      .count()
  }

  def loadSurveyDF(spark: SparkSession, dataFile: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataFile)
  }

  private def getSparkAppConf: SparkConf = {
    val sparkAppConf = new SparkConf
    //Set all Spark Configs
    val props = new Properties
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))

    sparkAppConf
  }

}