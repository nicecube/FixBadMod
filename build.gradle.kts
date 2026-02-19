/**
 * NOTE: This is entirely optional and basics can be done in `settings.gradle.kts`
 */
import org.gradle.jvm.tasks.Jar

group = "ca.nicecube"
version = "1.0.2"

val hytaleServerJarPath = providers.gradleProperty("hytale.server.jar")
    .orElse("../../HytaleServer.jar")
    .get()
val hytaleServerJar = layout.projectDirectory.file(hytaleServerJarPath).asFile

if (!hytaleServerJar.exists()) {
    throw GradleException(
        "Missing Hytale server jar at '$hytaleServerJarPath'. " +
            "Place it there or override with -Phytale.server.jar=<path>."
    )
}

configurations.configureEach {
    // The ScaffoldIt plugin adds com.hypixel.hytale:Server automatically.
    // We force local compilation against your provided jar instead.
    exclude(group = "com.hypixel.hytale", module = "Server")
}

repositories {
    // Any external repositories besides: MavenLocal, MavenCentral, HytaleMaven, and CurseMaven
}

dependencies {
    // Local Hytale server API jar (override with -Phytale.server.jar=...).
    compileOnly(files(hytaleServerJar))
}

tasks.named<Jar>("jar") {
    archiveFileName.set("FixBadMod-${project.version}.jar")
}
