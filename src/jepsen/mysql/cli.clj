(ns jepsen.mysql.cli
  "Command-line entry point for MySQL tests."
  (:require [clojure [string :as str]]
            [jepsen [checker :as checker]
             [cli :as cli]
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
                          [closed-predicate :as closed-predicate]
                          [db :as db]]
            [jepsen.mysql.db [mysql :as db.mysql]
                             [noop :as db.noop]
                             [rds :as db.rds]]))

(def db-types
  "A map of DB names to functions that take CLI options and return Jepsen DB
  instances."
  {:none  db.noop/db
   :maria db/db
   :mysql db.mysql/db
   :rds   db.rds/db})

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
                   :none  os/noop
                   :rds   os/noop
                   debian/os)
        ssh      (case (:db opts)
                   (:none :rds) {:dummy? true}
                   (:ssh tests/noop-test))
        nemesis  (case (:db opts)
                   (:none :rds) nil
                   (nc/nemesis-package
                     {:db db
                      :nodes (:nodes opts)
                      :faults (:nemesis opts)
                      :partition {:targets [:one :majority]}
                      :pause {:targets [:one :majority]}
                      :kill  {:targets [:one :majority :all]}
                      :interval (:nemesis-interval opts)}))]
    (merge tests/noop-test
           opts
           {:name (str (name workload-name)
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

   [nil "--key-count NUM" "Number of keys in active rotation."
    :default  10
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

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
                   (cli/serve-cmd))
            args))
