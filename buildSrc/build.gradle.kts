plugins {
    id("groovy-gradle-plugin")
}

repositories {
    gradlePluginPortal()
    mavenCentral()
}

dependencies {
    implementation("io.micronaut.gradle:micronaut-gradle-plugin:3.7.3")
}
