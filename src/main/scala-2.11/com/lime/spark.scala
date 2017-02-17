package com.lime

/**
	* Created by musa.bilal on 17/02/2017.
	*/
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object spark {

	val conf: SparkConf = new SparkConf().setAppName("outlier-detection")
		.setMaster("local[*]")
		.set("spark.driver.host", "127.0.0.1")
		.set("spark.driver.memory", "4g")
		.set("spark.executor.memory", "8g")
		.set("spark.sql.crossJoin.enabled", "true")
		.set("spark.sql.shuffle.partitions", "100")  //TODO: tweak and measure performance
		.set("spark.driver.maxResultSize", "4g")

	val session: SparkSession = SparkSession
		.builder()
		.config(conf)
		.getOrCreate()

	val context: SparkContext = session.sparkContext

	val sqlContext: SQLContext = session.sqlContext

}