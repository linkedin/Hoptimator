## Custom DDL Parser Codegen

These files are copy-pasted from Apache Calcite's `./server` module, with custom modifications.
There is a lot of complex codegen machinery in the Calcite project. Rather than replicating all
that here, it is easier to simply copy-paste these files back into the Calcite source tree and
build from there.

One way to do that:

  1. Checkout Apache Calcite, using whatever release branch Hoptimator is built against.
  2. Copy `./server/build.gradle.kts` into a new module, e.g. `./hoptimator-ddl/build.gradle.kts`.
  3. Modify the build script to use our package name:

    packageName.set("com.linkedin.hoptimator.jdbc.ddl")

  4. Update `settings.gradle` to include the new module.
  5. Run `./gradlew -PskipJavadoc assemble`.
  6. Copy the resulting Java source code back to Hoptimator:

    cp calcite/hoptimator-ddl/src/main/java/com/linkedin/hoptimator/jdbc/ddl/* Hoptimator/hoptimator-jdbc/src/main/java/com/linkedin/hoptimator/jdbc/ddl
    cp calcite/hoptimator-ddl/build/javacc/javaCCMain/com/linkedin/hoptimator/jdbc/ddl/* Hoptimator/hoptimator-jdbc/src/main/java/com/linkedin/hoptimator/jdbc/ddl

The resulting Java classes can be used within `HoptimatorDdlExecutor` etc.

