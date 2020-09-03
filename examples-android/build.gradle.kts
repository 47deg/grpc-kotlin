buildscript {
    repositories {
        gradlePluginPortal()
        google()
    }
    dependencies {
        classpath("com.android.tools.build:gradle:4.0.1")
    }
}

plugins {
    kotlin("jvm") version "1.3.72"
    id("org.jlleitschuh.gradle.ktlint") version "9.2.1"
    `maven-publish`
}

// todo: move to subprojects, but how?
ext["grpcVersion"] = "1.30.0"
// ext["grpcKotlinVersion"] = "0.1.3"
// To use the arrow fx coroutines implementation, remember to `publishToMavenLocal`
// in the root project
ext["grpcKotlinVersion"] = "0.2"
ext["protobufVersion"] = "3.12.2"

allprojects {
    repositories {
        mavenLocal()
        google()
        jcenter()
        mavenCentral()
        maven(url = "https://dl.bintray.com/arrow-kt/arrow-kt/")
        maven(url = "https://oss.jfrog.org/artifactory/oss-snapshot-local/") // for SNAPSHOT builds
    }

    apply(plugin = "org.jlleitschuh.gradle.ktlint")
}

tasks.replace("assemble").dependsOn(":server:installDist")
