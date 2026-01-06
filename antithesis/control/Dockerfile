FROM eclipse-temurin:21-alpine

# Install prereqs
RUN apk add bash git gnuplot graphviz grep

# Install test
WORKDIR /opt/jepsen-test
COPY jepsen-test.jar /opt/jepsen-test/jepsen-test.jar

# Test composer scripts
RUN mkdir -p /opt/antithesis/test/v1/jepsen
COPY op /opt/antithesis/test/v1/jepsen/parallel_driver_op
COPY check /opt/antithesis/test/v1/jepsen/eventually_check

# Antithesis instrumentation
RUN mkdir -p /opt/antithesis/catalog
# Instrumenting the JVM seems to cause some sort of JVM class/interface error
# in the pretty printer
#RUN ln -s /opt/jepsen-test /opt/antithesis/catalog

# Go
ENTRYPOINT java -Djava.awt.headless=true -Xmx8g \
  -ea \
  -cp '/opt/jepsen-test/*' \
  jepsen.mysql.cli \
  #-jar /opt/jepsen-test/jepsen-test.jar \
  antithesis \
  --antithesis \
  --nodes n1,n2,n3 \
  --workload append \
  --concurrency 6n \
  --isolation repeatable-read \
  --expected-consistency-model snapshot-isolation
