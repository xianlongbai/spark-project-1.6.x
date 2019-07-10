name := "spark"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.3.0"

//libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.8.2.2"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.0"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.3" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.3" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3" % "provided"

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.6.1"

libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.6"


//libraryDependencies ++= Seq(
//)