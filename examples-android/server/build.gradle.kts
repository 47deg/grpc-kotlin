plugins {
    application
    kotlin("jvm")
}

repositories {
    mavenLocal()
    google()
    jcenter()
    mavenCentral()
    maven(url = "https://dl.bintray.com/arrow-kt/arrow-kt/")
    maven(url = "https://oss.jfrog.org/artifactory/oss-snapshot-local/") // for SNAPSHOT builds
}

dependencies {
    implementation(kotlin("stdlib"))
    // This should be using Stream published through mavenLocal
    implementation(project(":common"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.3.7")
    //implementation("io.arrow-kt:arrow-fx-coroutines:0.11.0-SNAPSHOT")
    runtimeOnly("io.grpc:grpc-netty:${rootProject.ext["grpcVersion"]}")
}

application {
    mainClassName = "ServerKt"
}
