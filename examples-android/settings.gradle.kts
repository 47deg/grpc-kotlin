rootProject.name = "grpc-kotlin-sample-android"

include("common", "android", "server")
project(":common").projectDir = File("${rootDir}/common")
// when running the assemble task, ignore the android subproject
//if (startParameter.taskRequests.find { it.args.contains("assemble") } == null) {
//    include("common", "android", "server")
//} else {
//    include("common", "server")
//}
