# Flink for the laptop rehearsal: the same Flink version as EMR, with the Kafka SQL
# connector that EMR adds itself through kafka.enabled
FROM flink:1.20.0-java17
ADD --chown=flink:flink https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/
# The checkpoint volume takes this folder's owner, so Flink can write to it
RUN mkdir -p /checkpoints && chown flink:flink /checkpoints
