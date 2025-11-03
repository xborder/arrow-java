#!/usr/bin/env bash
set -euo pipefail

# Build and install updated artifacts so dependency resolution picks up local changes
mvn -q -DskipTests -Dcheckstyle.skip=true -Dspotbugs.skip=true -Dlicense.skip=true -Dspotless.skip=true -pl flight/flight-sql-jdbc-core -am install

# Build a classpath that includes test dependencies
CP_FILE=$(mktemp)
mvn -q -DskipTests -DincludeScope=test -pl flight/flight-sql-jdbc-core -am dependency:build-classpath -Dmdep.outputFile="${CP_FILE}"
# Prefer local module classes first to pick up uninstalled changes
CLASSPATH="vector/target/classes:flight/flight-core/target/classes:flight/flight-sql/target/classes:flight/flight-sql-jdbc-core/target/test-classes:flight/flight-sql-jdbc-core/target/classes:$(cat "${CP_FILE}")"
rm -f "${CP_FILE}"

# Java module opens needed by Arrow memory util
JAVA_OPTS=(
  --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
)
exec java "${JAVA_OPTS[@]}" -cp "${CLASSPATH}" org.apache.arrow.driver.jdbc.tools.UuidMockServerMain

