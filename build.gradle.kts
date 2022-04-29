import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.6.20"
}

group = "pce.bq.api"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.3.8")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.3.8")

    implementation(platform("com.google.cloud:libraries-bom:25.1.0"))
    implementation("com.google.cloud:google-cloud-bigquery")

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "11"
}
