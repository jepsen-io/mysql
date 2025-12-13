FROM eclipse-temurin:21-alpine
# Install prereqs
RUN apk add git gnuplot graphviz grep
# Install test
WORKDIR /opt/jepsen-test
COPY jepsen-test.jar /opt/jepsen-test/jepsen-test.jar
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
  test \
  --antithesis \
  --nodes n1,n2,n3 \
  --concurrency 6n \
  --workload append
