#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <port>" >&2
  exit 2
fi
PORT="$1"

# Build and install updated artifacts so dependency resolution picks up local changes
#mvn -q -DskipTests -Dcheckstyle.skip=true -Dspotbugs.skip=true -Dlicense.skip=true -Dspotless.skip=true -pl flight/flight-sql-jdbc-core -am install

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

# Run the JDBC client main class
exec java "${JAVA_OPTS[@]}" -cp "${CLASSPATH}" org.apache.arrow.driver.jdbc.tools.UuidJdbcClientMain "$PORT"

