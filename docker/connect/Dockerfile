FROM debezium/connect:2.4

# Install Confluent Avro converter for Schema Registry integration
ARG CONFLUENT_VERSION=7.5.0

USER root
RUN mkdir -p /kafka/connect/confluent-avro-converter && \
    cd /kafka/connect/confluent-avro-converter && \
    curl -sL https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/${CONFLUENT_VERSION}/kafka-connect-avro-converter-${CONFLUENT_VERSION}.jar -o kafka-connect-avro-converter-${CONFLUENT_VERSION}.jar && \
    curl -sL https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/${CONFLUENT_VERSION}/kafka-connect-avro-data-${CONFLUENT_VERSION}.jar -o kafka-connect-avro-data-${CONFLUENT_VERSION}.jar && \
    curl -sL https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/${CONFLUENT_VERSION}/kafka-schema-registry-client-${CONFLUENT_VERSION}.jar -o kafka-schema-registry-client-${CONFLUENT_VERSION}.jar && \
    curl -sL https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/${CONFLUENT_VERSION}/kafka-schema-serializer-${CONFLUENT_VERSION}.jar -o kafka-schema-serializer-${CONFLUENT_VERSION}.jar && \
    curl -sL https://packages.confluent.io/maven/io/confluent/kafka-schema-converter/${CONFLUENT_VERSION}/kafka-schema-converter-${CONFLUENT_VERSION}.jar -o kafka-schema-converter-${CONFLUENT_VERSION}.jar && \
    curl -sL https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/${CONFLUENT_VERSION}/kafka-avro-serializer-${CONFLUENT_VERSION}.jar -o kafka-avro-serializer-${CONFLUENT_VERSION}.jar && \
    curl -sL https://packages.confluent.io/maven/io/confluent/common-config/${CONFLUENT_VERSION}/common-config-${CONFLUENT_VERSION}.jar -o common-config-${CONFLUENT_VERSION}.jar && \
    curl -sL https://packages.confluent.io/maven/io/confluent/common-utils/${CONFLUENT_VERSION}/common-utils-${CONFLUENT_VERSION}.jar -o common-utils-${CONFLUENT_VERSION}.jar && \
    curl -sL https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.jar -o avro-1.11.3.jar && \
    curl -sL https://repo1.maven.org/maven2/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar -o guava-31.1-jre.jar && \
    curl -sL https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar -o failureaccess-1.0.1.jar
USER kafka
