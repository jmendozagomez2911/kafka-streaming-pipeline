## Goal
Set up a **Scala** project to program against Kafka using the **official Java client** (`kafka-clients`).

> Kafka’s official SDK is Java. Scala runs on the JVM and uses the same official client library. Other languages exist, but most are **community-supported** and may differ in behaviour/features.

---

## Prerequisites
- **IntelliJ IDEA Community** (or another IDE, but following along is easier with IntelliJ)
- **Java JDK 11** recommended (Amazon Corretto 11 is a common choice). Java 17 can work too.
- Scala support:
  - install/enable the **Scala plugin** in IntelliJ
- Build tool:
  - **Gradle** (recommended here for simpler syntax and fewer build mistakes)
  - Maven is fine too; code is the same, only build files differ.

---

## IntelliJ: create the project
1. **New Project** → choose **Gradle** + **Scala**
2. Select your **Project SDK** (JDK 11)
3. Name example: `kafka-beginners-course`
4. Example coordinates:
   - `groupId`: `io.conduktor.demos`
   - `artifactId`: `kafka-beginners-course`
   - `version`: `1.0-SNAPSHOT`

### Optional organisation: use subprojects/modules
A tidy approach is to create a module like `kafka-basics` and keep code there.

- Create module: `kafka-basics` (Gradle + Scala + Java 11)
- Use the `build.gradle` inside the module directory (e.g., `kafka-basics/build.gradle`)

---

## Add dependencies (Gradle)
You need:
- `org.apache.kafka:kafka-clients`
- `org.slf4j:slf4j-api`
- `org.slf4j:slf4j-simple` (simple logging backend for local demos)
- `org.scala-lang:scala-library` (if Gradle didn’t add it automatically)

Example `dependencies { ... }` (versions are examples; use a compatible set):
```gradle
dependencies {
  implementation "org.apache.kafka:kafka-clients:<version>"
  implementation "org.slf4j:slf4j-api:<version>"
  implementation "org.slf4j:slf4j-simple:<version>"

  // usually already included by the Scala plugin, but keep in mind:
  implementation "org.scala-lang:scala-library:<version>"
}
````

**Common pitfall:**
If you mistakenly add them as `testImplementation`, your app may compile but logging won’t work when running normally. For these demos, keep them as **implementation**.

---

## Load Gradle changes

After editing `build.gradle`, reload Gradle so IntelliJ downloads the jars:

* Use the Gradle tool window → **Reload/Refresh** (or “Load Gradle Changes”)

You should then see libraries under **External Libraries** (Kafka + SLF4J).

---

## Create a first object and run it

Create:

* Package: `io.conduktor.demos.kafka`
* Scala object: `ProducerDemo`

Add a `main`:

```scala
package io.conduktor.demos.kafka

object ProducerDemo {
  def main(args: Array[String]): Unit = {
    println("Hello world")
  }
}
```

Run it to validate your setup.

---

## IntelliJ setting (recommended for this course style)

To reduce Gradle-run quirks while coding:

* Settings/Preferences → Build, Execution, Deployment → Build Tools → Gradle

    * **Build and run using:** IntelliJ IDEA
    * **Run tests using:** Gradle (fine)

This keeps runs snappy and predictable while still using Gradle for dependency management.