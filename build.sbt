name := "KafkaSparkIntegration"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq("org.apache.spark" % "spark-sql_2.11" % "2.2.0",
                        "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.0",
                        "org.apache.kafka" % "kafka-clients" % "0.11.0.1")
libraryDependencies += "au.com.bytecode" % "opencsv" % "2.4"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0"