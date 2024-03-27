import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("org.springframework.boot") version "3.1.4"
    id("io.spring.dependency-management") version "1.1.3"
    id("org.jlleitschuh.gradle.ktlint") version "11.6.0"
    kotlin("jvm") version "1.8.22"
    kotlin("plugin.spring") version "1.8.22"
    kotlin("plugin.serialization") version "1.5.21"
}

group = "pt.isel.ps"
version = "0.0.1-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

dependencies {
    // For Spring
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")

    // For Postgresql
    implementation("org.postgresql:postgresql:42.7.0")

    // For JSON
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.3.0")

    // For automated tests
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webflux") // To use WebTestClient on tests
    testImplementation(kotlin("test"))
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs += "-Xjsr305=strict"
        jvmTarget = "17"
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

/**
 * DB related tasks
 * - To run `psql` inside the container, do
 *      docker exec -ti db-tests psql -d db -U dbuser -W
 *   and provide it with the same password as define on `tests/Dockerfile-db-test`
 */
task<Exec>("dbTestsUp") {
    commandLine("docker-compose", "up", "-d", "--build", "--force-recreate", "db-tests")
}

task<Exec>("dbTestsWait") {
    commandLine("docker", "exec", "db-tests", "/app/bin/wait-for-postgres.sh", "localhost")
    dependsOn("dbTestsUp")
}

task<Exec>("dbTestsDown") {
    commandLine("docker-compose", "down")
}

tasks.named("check") {
    dependsOn("dbTestsWait")
    finalizedBy("dbTestsDown")
}
