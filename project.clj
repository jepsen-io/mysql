(defproject jepsen.mysql "0.0.1"
  :description "Tests for MySQL with read replicas"
  :url "https://github.com/jepsen-io/jepsen"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.4"]
                 ; Antithesis needs a specific version and we're too new
                 [cheshire "5.3.1"
                  :exclusions [com.fasterxml.jackson.core/jackson-core
                               com.fasterxml.jackson.core/jackson-annotations
                               com.fasterxml.jackson.core/jackson-databind]]
                 [jepsen "0.3.11-SNAPSHOT"
                  :exclusions [com.fasterxml.jackson.core/jackson-databind
                               com.fasterxml.jackson.core/jackson-annotations
                               com.fasterxml.jackson.core/jackson-core]]
                 [io.jepsen/antithesis "0.1.0-SNAPSHOT"]
                 [http-kit "2.8.1"]
                 [com.github.seancorfield/next.jdbc "1.3.1070"]
                 [com.mysql/mysql-connector-j "9.5.0"]
                 [org.mariadb.jdbc/mariadb-java-client "3.5.6"]]
  :main jepsen.mysql.cli
  :repl-options {:init-ns jepsen.mysql.cli}
  :jvm-opts ["-Djava.awt.headless=true"
             ;"-agentpath:/home/aphyr/yourkit/bin/linux-x86-64/libyjpagent.so=disablestacktelemetry,exceptions=disable,delay=1000"
             "-server"])
