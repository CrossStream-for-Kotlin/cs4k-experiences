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
 * PostgreSQl DB related tasks.
 */
task<Exec>("postgresUp") {
    commandLine("docker-compose", "-f", "../docker-compose-prototype-option1.yaml", "up", "-d", "--build", "postgres")
}

task<Exec>("postgresWait") {
    commandLine("docker", "exec", "postgres", "/app/bin/wait-for-postgres.sh", "localhost")
    dependsOn("postgresUp")
}

task<Exec>("postgresDown") {
    commandLine("docker-compose", "-f", "../docker-compose-prototype-option1.yaml", "down")
}

/**
 * Redis related tasks
 */
task<Exec>("redisUp") {
    commandLine(
        "docker-compose",
        "-f",
        "../docker-compose-prototype-option2-redis.yaml",
        "up",
        "-d",
        "--build",
        "redis"
    )
}

task<Exec>("redisDown") {
    commandLine("docker-compose", "-f", "../docker-compose-prototype-option2-redis.yaml", "down")
}

/**
 * RabbitMQ related tasks
 */
task<Exec>("rabbitUp") {
    commandLine(
        "docker-compose",
        "-f",
        "../docker-compose-prototype-option2-rabbit.yaml",
        "up",
        "-d",
        "--build",
        "rabbit-mq"
    )
}

task<Exec>("rabbitClusterUp") {
    val command =
        arrayOf("docker-compose", "-f", "../docker-compose-prototype-option2-rabbit.yaml", "up", "-d", "--build")
    val services = command + arrayOf("rabbit-mq1", "rabbit-mq2", "rabbit-mq3")
    commandLine(*services)
}

task<Exec>("rabbitDown") {
    commandLine("docker-compose", "-f", "../docker-compose-prototype-option2-rabbit.yaml", "down")
}

/**
 * Prototype related tasks
 */
task<Copy>("extractUberJar") {
    dependsOn("assemble")
    // opens the JAR containing everything...
    from(zipTree("$buildDir/libs/app-$version.jar"))
    // ... into the 'build/dependency' folder
    into("build/dependency")
}

// Option 1

task<Exec>("prototypeOption1ComposeUp") {
    commandLine(
        "docker-compose",
        "-f",
        "../docker-compose-prototype-option1.yaml",
        "up",
        "--build",
        "--force-recreate",
        "--scale",
        "spring-service=3"
    )
    dependsOn("extractUberJar")
}

task<Exec>("prototypeOption1ComposeDown") {
    commandLine("docker-compose", "-f", "../docker-compose-prototype-option1.yaml", "down")
}

// Option 2

task<Exec>("prototypeOption2RabbitComposeUp") {
    commandLine(
        "docker-compose",
        "-f",
        "../docker-compose-prototype-option2-rabbit.yaml",
        "up",
        "--build",
        "--force-recreate",
        "--scale",
        "spring-service=3"
    )
    dependsOn("extractUberJar")
}

task<Exec>("prototypeOption2RabbitComposeDown") {
    commandLine("docker-compose", "-f", "../docker-compose-prototype-option2-rabbit.yaml", "down")
}

task<Exec>("prototypeOption2RedisComposeUp") {
    commandLine(
        "docker-compose",
        "-f",
        "../docker-compose-prototype-option2-redis.yaml",
        "up",
        "--build",
        "--force-recreate",
        "--scale",
        "spring-service=3"
    )
    dependsOn("extractUberJar")
}

task<Exec>("prototypeOption2RedisComposeDown") {
    commandLine("docker-compose", "-f", "../docker-compose-prototype-option2-redis.yaml", "down")
}

// Option 3

task<Exec>("prototypeOption3ComposeUp") {
    commandLine(
        "docker-compose",
        "-f",
        "../docker-compose-prototype-option3.yaml",
        "up",
        "--build",
        "--force-recreate",
        "--scale",
        "spring-service=2"
    )
    dependsOn("extractUberJar")
}

task<Exec>("prototypeOption3ComposeDown") {
    commandLine("docker-compose", "-f", "../docker-compose-prototype-option3.yaml", "down")
}
