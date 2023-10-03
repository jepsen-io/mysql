(ns jepsen.mysql.cli
  "Command-line entry point for MySQL tests."
  (:require [clojure [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
             [cli :as cli]
             [control :as c]
             [db :as jepsen.db]
             [generator :as gen]
             [nemesis :as nemesis]
             [os :as os]
             [tests :as tests]
             [util :as util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os.debian :as debian]
            [jepsen.mysql [append :as append]
                          [closed-predicate :as closed-predicate]]
            [jepsen.mysql.db [maria :as db.maria]
                             [mysql :as db.mysql]
                             [noop :as db.noop]]))

(def db-types
  "A map of DB names to functions that take CLI options and return Jepsen DB
  instances."
  {:none  db.noop/db
   :maria db.maria/db
   :mysql db.mysql/db})

(def workloads
  "A map of workload names to functions that take CLI options and return
  workload maps."
  {:append append/workload
   :closed-predicate closed-predicate/workload
   :none (fn [_] tests/noop-test)})

(def all-workloads
  "A collection of workloads we run by default."
  [])

(def all-nemeses
  "Combinations of nemeses for tests"
  [[]])

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none []
   :all  [:pause :kill :partition :clock]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(def short-isolation
  {:strict-serializable "Strict-1SR"
   :serializable        "S"
   :strong-snapshot-isolation "Strong-SI"
   :snapshot-isolation  "SI"
   :repeatable-read     "RR"
   :read-committed      "RC"
   :read-uncommitted    "RU"})

(defn mysql-test
  "Given options from the CLI, constructs a test map."
  [opts]
  (let [workload-name (:workload opts)
        workload ((workloads workload-name) opts)
        db       ((db-types (:db opts)) opts)
        os       (case (:db opts)
                   :none os/noop
                   debian/os)
        ssh      (case (:db opts)
                   :none {:dummy? true}
                   (:ssh tests/noop-test))
        nemesis  (case (:db opts)
                   :none nil
                   (nc/nemesis-package
                     {:db db
                      :nodes (:nodes opts)
                      :faults (:nemesis opts)
                      :partition {:targets [:one :majority]}
                      :pause {:targets [:one]}
                      :kill  {:targets [:one :all]}
                      :interval (:nemesis-interval opts)}))]
    (merge tests/noop-test
           opts
           {:name (str (name (:db opts))
                       " " (name workload-name)
                       (when (:lazyfs opts) " lazyfs")
                       " binlog=" (name (:binlog-format opts))
                       " " (short-isolation (:isolation opts)) "("
                       (short-isolation (:expected-consistency-model opts)) ") "
                       (str/join "," (map name (:nemesis opts))))
            :ssh ssh
            :os os
            :db db
            :checker (checker/compose
                       {:perf (checker/perf
                                {:nemeses (:perf nemesis)})
                        :clock (checker/clock-plot)
                        :stats (checker/stats)
                        :exceptions (checker/unhandled-exceptions)
                        :timeline (timeline/html)
                        :workload (:checker workload)})
            :client    (:client workload)
            :nemesis   (:nemesis nemesis nemesis/noop)
            :generator (->> (:generator workload)
                            (gen/stagger (/ (:rate opts)))
                            (gen/nemesis (:generator nemesis))
                            (gen/time-limit (:time-limit opts)))})))

(def cli-opts
  "Command line options"
  [[nil "--binlog-format FORMAT" "What binlog format should we use?"
    :default :mixed
    :parse-fn keyword
    :validate [#{:mixed :statement :row} "must be statement, mixed, or row"]]

   ["-d" "--db TYPE" "Maria, mysql, or none (for testing an extant cluster)."
    :default :maria
    :parse-fn keyword
    :validate [db-types (cli/one-of (keys db-types))]]

   ["-i" "--isolation LEVEL" "What level of isolation we should set: serializable, repeatable-read, etc."
    :default :serializable
    :parse-fn keyword
    :validate [#{:read-uncommitted
                 :read-committed
                 :repeatable-read
                 :serializable}
               "Should be one of read-uncommitted, read-committed, repeatable-read, or serializable"]]

   [nil "--expected-consistency-model MODEL" "What level of isolation do we *expect* to observe? Defaults to the same as --isolation."
    :default nil
    :parse-fn keyword]

   [nil "--innodb-flush-log-at-trx-commit SETTING" "0 for write+flush n seconds, 1 for every txn commit, 2 for write at commit, flush every ns econds."
    :default 1
    :parse-fn parse-long]

   [nil "--insert-only" "If set, tells certain workloads (e.g. closed-predicate) to perform only inserts."
    :id :insert-only?]

   [nil "--key-count NUM" "Number of keys in active rotation."
    :default  10
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--lazyfs" "If set, mounts MySQL in a lazy filesystem that loses un-fsyned writes on nemesis kills."]

   ["-l" "--log-sql" "If set, logs selected SQL statements to the console to aid in debugging"]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause :kill :partition :clock})
               "Faults must be pause, kill, partition, clock, or member, or the special faults all or none."]]

   [nil "--max-txn-length NUM" "Maximum number of operations in a transaction."
    :default  4
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Maximum number of writes to any given key."
    :default  256
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default 5
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--prepare-threshold INT" "Passes a prepareThreshold option to the JDBC spec."
    :parse-fn parse-long]

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default 100
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--replica-preserve-commit-order MODE" "Either on or off"
    :default "ON"
    :parse-fn #(.toUpperCase %)
    :validate [#{"ON" "OFF"} "Must be `on` or `off`"]]

   [nil "--repro-112446" "For the closed-predicate workload, uses a generator more likely to generate compact reproductions of MySQL bug 112446: fractured reads at serializable."]

   ["-v" "--version STRING" "What version of Stolon should we test?"
    :default "0.16.0"]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :missing  (str "Must specify a workload: " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]
   ])

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [opts]
  (let [nemeses   (if-let [n (:nemesis opts)] [n] all-nemeses)
        workloads (if-let [w (:workload opts)] [w] all-workloads)]
    (for [n nemeses, w workloads, i (range (:test-count opts))]
      (mysql-test (assoc opts :nemesis n :workload w)))))

(defn opt-fn
  "Transforms CLI options before execution."
  [parsed]
  (update-in parsed [:options :expected-consistency-model]
             #(or % (get-in parsed [:options :isolation]))))

(def wipe-command
  {"wipe"
   {:opt-spec [[nil "--nodes NODE_LIST" "Comma-separated list of node hostnames."
                :parse-fn #(str/split % #",\s*")]]
    :opt-fn identity
    :usage "MySQL can get wedged in completely inscrutable ways. This command
           completely uninstalls it on the given nodes."
    :run (fn [{:keys [options]}]
           (info (pr-str options))
           (c/on-many (:nodes options)
                      (info "Wiping")
                      (c/su
                        (c/exec "DEBIAN_FRONTEND='noninteractive'"
                                :apt :remove :-y :--purge
                                (c/lit "mysql-*")
                                (c/lit "mariadb-*"))
                        (c/exec :rm :-rf "/var/lib/mysql"
                                (c/lit "/var/lib/mysql-*")
                                "/var/log/mysql"
                                "/etc/mysql"))
                      (info "Wiped")))}})

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  mysql-test
                                         :opt-spec cli-opts
                                         :opt-fn   opt-fn})
                   (cli/test-all-cmd {:tests-fn all-tests
                                      :opt-spec cli-opts
                                      :opt-fn   opt-fn})
                   (cli/serve-cmd)
                   wipe-command)
            args))
