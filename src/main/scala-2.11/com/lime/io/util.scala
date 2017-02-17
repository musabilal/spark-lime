package com.lime.io

/**
	* Created by musa.bilal on 17/02/2017.
	*/
import com.lime.spark.sqlContext
import org.apache.spark.sql.DataFrame

object io {

	def loadDf(filePath: String): DataFrame = {
		sqlContext.read
			.format("com.databricks.spark.csv")
			.option("header", "true")
			.option("inferSchema", "false")
			.option("nullValue", "null")
			.option("treatEmptyValuesAsNulls", "true")
			.option("delimiter", ",")
			.option("mode", "FAILFAST")
			//        .option("quote", null)
			.load(filePath)
	}

}
