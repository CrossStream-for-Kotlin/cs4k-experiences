import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("org.springframework.boot") version "3.1.4"
    id("io.spring.dependency-management") version "1.1.3"
    id("org.jlleitschuh.gradle.ktlint") version "11.6.0"
    kotlin("jvm") version "1.8.22"
    kotlin("plugin.spring") version "1.8.22"
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
    // For coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.4")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive:1.6.4")

    // For Spring and JSON
    implementation("org.springframework.boot:spring-boot-starter-web")
    // see 'https://github.com/spring-projects/spring-framework/issues/31140'
    implementation("org.apache.tomcat.embed:tomcat-embed-core:10.1.17")
    implementation("org.apache.tomcat.embed:tomcat-embed-websocket:10.1.17")
    implementation("org.apache.tomcat.embed:tomcat-embed-el:10.1.17")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")

    // For Postgresql
    implementation("org.postgresql:postgresql:42.7.0")

    // For RabbitMQ
    implementation("com.rabbitmq:amqp-client:5.21.0")

    // For Redis
    implementation("redis.clients:jedis:5.1.0")
    implementation("io.lettuce:lettuce-core:6.3.2.RELEASE")

    // For automated tests
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webflux") // To use WebTestClient on tests
    testImplementation(kotlin("test"))

    // For HikariCP
    implementation("com.zaxxer:HikariCP:5.1.0")
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

tasks.named("check") {
    dependsOn("dbTestsWait")
    finalizedBy("dbTestsDown")
}

/**
 * PostgreSQl DB related tasks
 * - To run `psql` inside the container, do
 *      docker exec -ti db-tests psql -d db -U dbuser -W
 *   and provide it with the same password as define on `tests/Dockerfile-db-test`
 */
task<Exec>("dbTestsUp") {
    commandLine("docker-compose", "up", "-d", "--build", "db-tests")
}

task<Exec>("dbTestsWait") {
    commandLine("docker", "exec", "db-tests", "/app/bin/wait-for-postgres.sh", "localhost")
    dependsOn("dbTestsUp")
}

task<Exec>("dbTestsDown") {
    commandLine("docker-compose", "down")
}

/**
 * Redis related tasks
 */
task<Exec>("redisUp") {
    commandLine("docker-compose", "-f", "../docker-compose-redis.yaml", "up", "-d", "--build", "redis")
}

task<Exec>("redisDown") {
    commandLine("docker-compose", "-f", "../docker-compose-redis.yaml", "down")
}

/**
 * RabbitMQ related tasks
 */
task<Exec>("rabbitUp") {
    commandLine("docker-compose", "up", "-d", "--build", "rabbit-mq")
}

task<Exec>("rabbitClusterUp") {
    val command = arrayOf("docker-compose", "up", "-d", "--build")
    val services = command + arrayOf("rabbit-mq1", "rabbit-mq2", "rabbit-mq3", "rabbit-mq4", "rabbit-mq5", "rabbit-mq6")
    commandLine(*services)
}

task<Exec>("rabbitDown") {
    commandLine("docker-compose", "down")
}

/**
 * Demonstration related tasks
 */
task<Copy>("extractUberJar") {
    dependsOn("assemble")
    // opens the JAR containing everything...
    from(zipTree("$buildDir/libs/app-$version.jar"))
    // ... into the 'build/dependency' folder
    into("build/dependency")
}

task<Exec>("composeUp") {
    commandLine("docker-compose", "up", "--build", "--force-recreate", "--scale", "spring-service=3")
    dependsOn("extractUberJar")
}

task<Exec>("composeDown") {
    commandLine("docker-compose", "down")
}
