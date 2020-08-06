# JDBC Connectors for Apache Hive and Presto on Cloud Dataproc

## Running tests
* To run all unit tests, use `./gradlew test`
* To run specific unit test, use `./gradlew test --tests="com.google.cloud.dataproc.jdbc.{test name}"`
* To run system test, use
`./gradlew systemTest --tests="com.google.cloud.dataproc.jdbc.DataprocSystemTest"  -DprojectId="{projectId}" -Dregion="{region}" -Duser="{user}"`<br />
Note that projectId and region are the parameters for the specific project you are working on; user can be an arbitrary username, used as a prefix for the cluster name to create, delete and avoid cluster conflict during testing.
