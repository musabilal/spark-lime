package com.lime.lib
import breeze.linalg.*
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{Bucketizer, StringIndexer}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.sql.functions.{pow,sqrt}

import scalaz.Tags.MaxVal

/**
	* Created by musa.bilal on 20/02/2017.
	*/

object implicits {

	implicit class RichFeatureSet(featureSet: DataFrame) {

		def getFeatureTypes(columns: Seq[String]): Seq[(String, String)] = {
			columns.map{

				c =>
					val distinctValues = featureSet.select(col(c)).distinct().collect().length
					if (distinctValues == 2) (c, "Boolean")
					else if (distinctValues == 1) (c, "Static Column")
					else if (distinctValues > 2 && distinctValues < 10) (c, "Categorical")
					else if (distinctValues == featureSet.count()) (c, "ID Column")
					else (c, "Numerical")
			}
		}

		//=============================================//
		//             Feature Pre-processing          //
		//=============================================//

		/**
			* Creates dummy variables for all the categories besides the most frequently
			* occusing one in a categorical variable
			*
			* @param categoricalVariables A Sequence of predefined categorical variables in the dataframe
			* @param threshold            If categorical variables in the dataframe are not known beforehand, define
			*                             threshold value here, any columns that have a ratio of distinct values over
			*                             total rows in dataframe below the given threshold will be considered categorical
			*/
		def BinarizeCategoricalFeatures(categoricalVariables: Seq[String], threshold: Double): DataFrame = {

			val combinedCategoricalVariables = categoricalVariables

			val indexedDataFrame = combinedCategoricalVariables.foldLeft(featureSet) { (dataframe, variable) =>
				val str = new StringIndexer().setInputCol(variable).setOutputCol(variable + "Indexed").fit(dataframe)
				str.transform(dataframe)
			}

			val indexedCatVars = combinedCategoricalVariables.map(_ + "Indexed")

			val colsToSelect: Seq[Column] = indexedCatVars.flatMap {
				c =>
					val df = indexedDataFrame
						.groupBy(c, c.slice(0, c.length - 7))
						.agg(count(c).as("counts"))
						.orderBy(asc("counts")).collect().toSeq

					df.slice(0, df.length - 1)
						.map(row =>
							udfs.createDummyFeature(row.getDouble(0))(col(c))
								.as(c.slice(0, c.length - 7)+"_"+row.getString(1).replaceAll("[^a-zA-Z0-9]", "")))
			}

			val dfCat = indexedDataFrame.select(featureSet.columns.filter(!categoricalVariables.contains(_)).map(col) ++ colsToSelect: _*)

			val minCount = indexedDataFrame.count().toDouble * threshold

			val dummyVarsFiltered = dfCat.columns.filter(c => !featureSet.columns.contains(c) && dfCat.filter(col(c) === 1).count().toDouble >= minCount)

			dfCat.select((featureSet.columns.filter(!categoricalVariables.contains(_)) ++ dummyVarsFiltered).map(col(_)): _*)

		}

		def standardizeNumericalFeatures(numericalCols: Seq[String]): DataFrame = {

			val castedFeatureSet = featureSet.select(numericalCols.map(col(_).cast(DoubleType)) ++ featureSet.columns.diff(numericalCols).map(col): _*)

			val endPoints = numericalCols
				.flatMap(
					c => castedFeatureSet.select(min(c), max(c)).collect().map(a => (c, a.getDouble(0), a.getDouble(1))))

			val standardizedCols = numericalCols.map(c => {
				val minMaxList = endPoints.filter(_._1 == c).head
				udfs.standardizeFeatures(minMaxList._2, minMaxList._3)(col(minMaxList._1)).as(minMaxList._1)
			})

			featureSet.select(
				featureSet.columns.diff(numericalCols)
					.map(col)
					++
					standardizedCols: _*)
		}

		def approxBinSize(nSample: Long): Int = math.round(math.sqrt(featureSet.count()/nSample)).toInt

		def buildBinBoundaries: Array[Double] = {
			val bins = approxBinSize(1000)
			val step = 1 / bins
			Array(Double.NegativeInfinity, 0.25, 0.50, 0.75, Double.PositiveInfinity)
		}

		def discretizeNumericalFeatures(numericalFeatures: Seq[String], idVars: Seq[String]): DataFrame = {

			val bucketizedFeatures = numericalFeatures.foldLeft(featureSet) { (df, col) =>
				val bucketizer = new Bucketizer()
					.setInputCol(col)
					.setOutputCol(col+"_binned")
					.setSplits(buildBinBoundaries)

				bucketizer.transform(df)
			}

			bucketizedFeatures.select(idVars.map(col) ++ numericalFeatures.map(c => col(c + "_binned").as(c)): _*)
		}

		def combineBuckets(idVars: Seq[String]): DataFrame = {
			featureSet.withColumn("groupedBuckets",
				featureSet.columns
					.filter(!idVars.contains(_)).map(col(_).cast(IntegerType).cast(StringType))
				.reduce(concat(_ , _)))
		}

		def groupedBucketCenters(originalFeatureSet: DataFrame, joinKeys: Seq[String]): DataFrame = {
			val centeredColumns = originalFeatureSet.columns.filter(!(joinKeys ++ List("groupedBuckets")).contains(_)).map(c => mean(col(c)).as(c))

			featureSet.select("bidder_id", "groupedBuckets").join(originalFeatureSet, Seq(joinKeys:_*))
				.groupBy("groupedBuckets").agg(centeredColumns.head, centeredColumns.tail: _*)
		}

		def measureBucketDistance: DataFrame = {
			val featureSetA = featureSet.select(featureSet.columns.map(c => col(c).as(c+"_a")): _*)
			val featureSetB = featureSet.select(featureSet.columns.map(c => col(c).as(c+"_b")): _*)

			val distanceCalc = featureSet.columns.filter(!List("groupedBuckets").contains(_)).map(c => pow(col(c+"_a") - col(c+"_b"), 2).as(c+"_dist"))

			val centerDistances = featureSetA.join(featureSetB).select(Array(col("groupedBuckets_a"), col("groupedBuckets_b")) ++ distanceCalc: _*)

			centerDistances.withColumn("total_distance",
				centerDistances.columns
					.filter(!Seq("groupedBuckets_a", "groupedBuckets_b").contains(_))
					.map(col)
					.reduce(_ + _))
				.withColumn("distance", sqrt("total_distance"))
			  .select("groupedBuckets_a", "groupedBuckets_b", "distance")
		}

		def lime(): DataFrame = {

		}

	}


}

object udfs {

	def createDummyFeature(referenceValue: Double) = udf { (value: Double) => if (value == referenceValue) 1 else 0 }

	def standardizeFeatures(minVal: Double, maxVal: Double) = udf{ (x: Double) => (x - minVal) / (maxVal - minVal)}

}

