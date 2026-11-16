plugins {
    kotlin("jvm") version "2.0.21"
    kotlin("plugin.serialization") version "2.0.21"
    application
}

group = "com.synerise"
version = "1.0.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.ktor:ktor-server-core-jvm:3.0.1")
    implementation("io.ktor:ktor-server-netty-jvm:3.0.1")
    implementation("io.ktor:ktor-server-content-negotiation-jvm:3.0.1")
    implementation("io.ktor:ktor-serialization-kotlinx-json-jvm:3.0.1")
    implementation("ch.qos.logback:logback-classic:1.5.12")
    implementation("org.postgresql:postgresql:42.7.4")
    implementation("com.zaxxer:HikariCP:6.2.1")
}

application {
    mainClass.set("com.synerise.ingestion.ApplicationKt")
}

kotlin {
    jvmToolchain(21)
}


/* 
// Section enabling extensions (plugins) needed to build the project
plugins {
    // Enable support for the Kotlin language on the Java Virtual Machine (version 2.0.21)
    // Enable tool for automatic data conversion to and from JSON format (version 2.0.21)
    // Mark this project as a standalone, runnable application
}

// Set the main group name / company identifier for the project to "com.synerise"
// Set the version number of this application to "1.0.0"

// Section specifying where the program should download external libraries from
repositories {
    // Download all required add-ons from the main public repository, Maven Central
}

// Section containing the list of external libraries (ready-made tools) used by the program
dependencies {
    // Add the core Ktor web server engine (version 3.0.1)
    // Add the Netty network server to handle HTTP traffic for Ktor (version 3.0.1)
    // Add mechanism for automatic recognition of transferred data types like JSON (version 3.0.1)
    // Add tool for translating Kotlin objects into JSON text for the Ktor server (version 3.0.1)
    // Add the Logback library for recording logs and error messages (version 1.5.12)
    // Add driver enabling connection to a PostgreSQL database (version 42.7.4)
    // Add the HikariCP tool for fast management of database connection pools (version 6.2.1)
}

// Section for application startup settings
application {
    // Specify the entry point file where the program should start executing ("com.synerise.ingestion.ApplicationKt")
}

// Section for Kotlin compiler settings
kotlin {
    // Enforce the use of Java version 21 to compile and run this code
}

*/
