import AssemblyKeys._

assemblySettings

name := "marlin"

version := "0.2-SNAPSHOT"

scalaVersion := "2.10.4"

libraryDependencies  ++= Seq(
            "org.scalanlp" %% "breeze" % "0.7",
            "org.scalanlp" %% "breeze-natives" % "0.7",
	        "org.apache.spark" %% "spark-core" % "1.0.1" % "provided",
            "org.apache.hadoop" % "hadoop-client" % "2.3.0" % "provided",
            "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "netlib Repository" at "http://repo1.maven.org/maven2/"

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"

mergeStrategy in assembly <<= (mergeStrategy in assembly) {
  (old) => {
    case PathList("org", "apache", xs@_*) => MergeStrategy.first
    case PathList("scala", "reflect", xs@_*) => MergeStrategy.first
    case x => old(x)
  }
}