ARG CP_VERSION=8.2.0
FROM confluentinc/cp-server-connect:${CP_VERSION}

ARG DEBEZIUM_SQLSERVER_VERSION=3.1.2
ARG JDBC_CONNECTOR_VERSION=latest
ARG DB2_JDBC_VERSION=11.5.9.0

USER root
RUN confluent-hub install --no-prompt debezium/debezium-connector-sqlserver:${DEBEZIUM_SQLSERVER_VERSION} \
 && confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:${JDBC_CONNECTOR_VERSION} \
 && curl -fsSL -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/db2jcc4.jar \
      https://repo1.maven.org/maven2/com/ibm/db2/jcc/${DB2_JDBC_VERSION}/jcc-${DB2_JDBC_VERSION}.jar \
 && chown -R appuser:appuser /usr/share/confluent-hub-components
USER appuser
