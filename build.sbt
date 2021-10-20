name := "FlinkConversation"

version := "0.1"

scalaVersion := "2.12.15"
val flinkVersion = "1.14.0"
libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-runtime-web" % flinkVersion,
)
