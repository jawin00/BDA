name := "lead-time-analyzer"
version := "0.1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.3"

// Spark SQL — Linking subtag: this dependency is the canonical "we linked
// against Spark SQL" deliverable.
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided
)

// Single fat jar to make spark-submit easy.
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _ @ _*) => MergeStrategy.discard
  case _                            => MergeStrategy.first
}

assembly / assemblyJarName := "lead-time-analyzer-assembly.jar"
