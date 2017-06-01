
name := "stock-guru"
version := "0.0.1"
scalaVersion := "2.10.5"
// additional libraries
libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
"org.apache.spark" % "spark-streaming_2.10" % "2.1.0",
"org.apache.spark" % "spark-streaming-twitter_2.10" % "1.6.3",
"org.elasticsearch" % "elasticsearch-spark-13_2.10" % "5.4.0",
"edu.stanford.nlp" % "stanford-corenlp" % "3.5.1",
"edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" classifier "models"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
   case x => MergeStrategy.first
}
