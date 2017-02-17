name := "spark-lime"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core"  % "2.0.0",
	"org.apache.spark" %% "spark-mllib" % "2.0.0",
	"org.apache.spark" %% "spark-sql"   % "2.0.0",
	"org.apache.spark" %% "spark-hive"  % "2.0.0",
	"org.vegas-viz" 	 %% "vegas-spark" % "0.3.6"
)