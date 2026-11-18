import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins { id("com.android.application") }

val pathweftAbis = providers.gradleProperty("pathweftAbis").orElse("arm64-v8a").get().split(",")
require(pathweftAbis.isNotEmpty() && pathweftAbis.all { it in setOf("arm64-v8a", "x86_64") }) {
    "pathweftAbis must contain arm64-v8a and/or x86_64"
}

android {
    namespace = "org.estudely.pathweft"
    compileSdk = 37
    ndkVersion = "28.2.13676358"
    defaultConfig {
        applicationId = "org.estudely.pathweft"
        minSdk = 26
        targetSdk = 37
        versionCode = 1
        versionName = "0.1.0"
        ndk { abiFilters += pathweftAbis }
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }
    buildFeatures { buildConfig = true }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}
kotlin { compilerOptions { jvmTarget.set(JvmTarget.JVM_17) } }

tasks.register("checkRustLibrary") {
    doLast {
        for (abi in pathweftAbis) {
            check(file("src/main/jniLibs/$abi/libpathweft_android.so").exists()) {
                "Build the shared Rust library first: python3 scripts/build_android.py --abi $abi"
            }
        }
    }
}
tasks.named("preBuild") { dependsOn("checkRustLibrary") }

dependencies {
    // QR decoding for contact cards: pure Java, Apache-2.0, no Google Play Services, no runtime dependencies.
    implementation("com.google.zxing:core:3.5.4")
    testImplementation("junit:junit:4.13.2")
    androidTestImplementation("androidx.test:runner:1.7.0")
    androidTestImplementation("androidx.test.uiautomator:uiautomator:2.3.0")
}
