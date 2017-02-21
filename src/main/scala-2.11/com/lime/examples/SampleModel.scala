package com.lime.examples
import com.lime.lib.implicits._
import org.apache.spark.sql.functions.col

/**
	* Created by musa.bilal on 20/02/2017.
	*/
object SampleModel extends App{

	// Minimum number of samples required to build the local classifier
	val N = 1000
	val numericalFeatures: Seq[String] = Seq("col4","col5","col6","col7","col8")
	val booleanFeatures: Seq[String] = Seq()
	val idVariables: Seq[String] = Seq("bidder_id")
	val targetVariable: String = "prediction"

	val df = com.lime.io.io.loadDf("data/predictions.csv")

	val dfStandard = df
		.standardizeNumericalFeatures(numericalFeatures)
		.select((numericalFeatures ++ idVariables).map(col): _*)

	val centers = dfStandard
		.discretizeNumericalFeatures(numericalFeatures, idVariables)
		.combineBuckets(idVariables)
		.groupedBucketCenters(dfStandard, idVariables)

	val bucketDistances = centers.measureBucketDistance

	bucketDistances.show()


}
