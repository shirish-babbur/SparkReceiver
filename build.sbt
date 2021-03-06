name := "SparkReceiver"
version := "1.0.0"
scalaVersion := "2.11.6"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.0"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.6"
