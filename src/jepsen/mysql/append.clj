(ns jepsen.mysql.append
  "A test for transactional list append."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]
                     [set :as set]
                     [string :as str]]
            [dom-top.core :refer [loopr with-retry]]
            [elle.core :as elle]
            [jepsen [antithesis :as antithesis]
                    [checker :as checker]
                    [client :as client]
                    [core :as jepsen]
                    [generator :as gen]
                    [util :as util :refer [timeout]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle.append :as append]
            [jepsen.mysql [client :as c]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sqlb]
            [clj-commons.slingshot :refer [try+ throw+]]))

(def default-table-count 3)

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "txn" i))

(defn table-for
  "What table should we use for the given key?"
  [table-count k]
  (table-name (mod (hash k) table-count)))

(defn append-using-on-dup!
  "Appends an element to a key using an INSERT ... ON DUPLICATE KEY UPDATE
  statement."
  [conn test table k e]
  (j/execute!
    conn
    [(str "insert into " table " as t"
          " (id, sk, val) values (?, ?, ?)"
          " on duplicate key update"
          " val = CONCAT(t.val, ',', ?)")
     k k e e]))

(defn insert!
  "Performs an initial insert of a key with initial element e. Catches
  duplicate key exceptions, returning true if succeeded. If the insert fails
  due to a duplicate key, it'll break the rest of the transaction, assuming
  we're in a transaction, so we establish a savepoint before inserting and roll
  back to it on failure."
  [conn test txn? table k e]
  (try
    ;(info (if txn? "" "not") "in transaction")
    (when txn? (j/execute! conn ["savepoint upsert"]))
    (j/execute! conn
                [(str "insert into " table " (id, sk, val)"
                      " values (?, ?, ?)")
                 k k e])
    (when txn? (j/execute! conn ["release savepoint upsert"]))
    true
    (catch java.sql.SQLIntegrityConstraintViolationException e
      (if (re-find #"Duplicate entry" (.getMessage e))
        (do (info (if txn? "txn") "insert failed: " (.getMessage e))
            (when txn? (j/execute! conn ["rollback to savepoint upsert"]))
            false)
        (throw e)))))

(defn update!
  "Performs an update of a key k, adding element e. Returns true if the update
  succeeded, false otherwise."
  [conn test table k e]
  (let [res (-> conn
                (j/execute-one! [(str "update " table " set val = CONCAT(val, ',', ?)"
                                      " where id = ?") e k]))]
    (-> res
        :next.jdbc/update-count
        pos?)))

(defn mop!
  "Executes a transactional micro-op on a connection. Returns the completed
  micro-op."
  [conn test txn? [f k v]]
  (let [table-count (:table-count test default-table-count)
        table (table-for table-count k)]
    (Thread/sleep (long (rand-int 10)))
    [f k (case f
           :r (let [r (j/execute! conn
                                  [(str "select (val) from " table " where "
                                        ;(if (< (rand) 0.5) "id" "sk")
                                        "id"
                                        " = ? ")
                                   k]
                                  {:builder-fn rs/as-unqualified-lower-maps})]
                (when-let [v (:val (first r))]
                  (mapv parse-long (str/split v #","))))

           :append
           (let [vs (str v)]
             (if (:on-conflict test)
               ; Use ON CONFLICT
               (append-using-on-dup! conn test table k vs)
               ; Try an update, and if that fails, back off to an insert.
               (or (update! conn test table k vs)
                   ; No dice, fall back to an insert
                   (insert! conn test txn? table k vs)
                   ; OK if THAT failed then we probably raced with another
                   ; insert; let's try updating again.
                   (update! conn test table k vs)
                   ; And if THAT failed, all bets are off. This happens even
                   ; under SERIALIZABLE, but I don't think it technically
                   ; VIOLATES serializability.
                   (throw+ {:type     ::homebrew-upsert-failed
                            :key      k
                            :element  v})))
             v))]))

; initialized? is an atom which we set when we first use the connection--we set
; up initial isolation levels, logging info, etc. This has to be stateful
; because we don't necessarily know what process is going to use the connection
; at open! time.
(defrecord Client [node conn initialized?]
  client/Client
  (open! [this test node]
    (let [c (try (c/open test node)
                 (catch Throwable t
                   ; Slow down reconnects
                   (Thread/sleep 1000)
                   (throw t)))]
      (assoc this
             :node          node
             :conn          c
             :initialized?  (atom false))))

  (setup! [_ test]
    (when (= (jepsen/primary test) node)
      (when (compare-and-set! initialized? false true)
        (dotimes [i (:table-count test default-table-count)]
          (j/execute! conn
                      [(str "create table if not exists " (table-name i)
                            " (id int not null primary key,
                            sk int not null,
                            val text)")])
          ; Make sure we start fresh--in case we're using an existing
          ; cluster and the DB automation isn't wiping the state for us.
          (j/execute! conn [(str "delete from " (table-name i))])))))

  (invoke! [_ test op]
    ; One-time connection setup
    (when (compare-and-set! initialized? false true)
      (c/set-transaction-isolation! conn (:isolation test)))

    (c/with-errors op
      (timeout 10000 (throw+ {:type :timeout})
               (let [txn       (:value op)
                     use-txn?  (< 1 (count txn))
                     txn'      (if use-txn?
                                 ;(if true
                                 (j/with-transaction [t conn
                                                      {:isolation (:isolation test)}]
                                   (mapv (partial mop! t test true) txn))
                                 (mapv (partial mop! conn test false) txn))]
                 (assoc op :type :ok, :value txn')))))

  (teardown! [_ test])

  (close! [this test]
    (c/close! conn)))

(defn read-only
  "Converts writes to reads."
  [op]
  (loopr [txn' []]
         [[f k v :as mop] (:value op)]
         (recur (conj txn' (case f
                             :r mop
                             [:r k nil])))
         (assoc op :value txn')))

(defn on-follower?
  "Is the given operation going to execute on a follower?"
  [test op]
  (not= 0 (mod (:process op) (count (:nodes test)))))

(defn ro-gen
  "Nothing in standard MySQL replication stops you from writing to a secondary,
  which is, uh, exciting. We'll set up our generator to *only* emit reads to
  any non-primary node."
  [gen]
  (reify gen/Generator
    (update [this test ctx event]
      (ro-gen (gen/update gen test ctx event)))

    (op [this test ctx]
      (when-let [[op gen'] (gen/op gen test ctx)]
        (cond (= :pending op)
              [:pending this]

              (on-follower? test op)
              [(read-only op) (ro-gen gen')]

              true
              [op (ro-gen gen')])))))

(defn antithesis-checker
  "Wraps the normal Elle checker in one that allows unknown outcomes, for
  Antithesis."
  [checker]
  (if (antithesis/antithesis?)
    (reify checker/Checker
      (check [this test history opts]
        (let [res (checker/check checker test history opts)
              ; Empty transaction graphs would normally indicate a broken test,
              ; but Antithesis does this all the time.
              res (if (empty? (set/difference (set (:anomaly-types res))
                                              #{:empty-transaction-graph}))
                    (assoc res :valid? true)
                    res)]
          ; We always want to pass
          (antithesis/assert-always (true? (:valid? res))
                                    "elle valid"
                                    res)
          res)))
    checker))

(defn workload
  "A list append workload."
  [opts]
  (-> (append/test (assoc (select-keys opts [:key-count
                                             :max-txn-length
                                             :max-writes-per-key])
                          :min-txn-length 1
                          :consistency-models [(:expected-consistency-model opts)]))
      (assoc :client (Client. nil nil nil))
      ; Galera lets us write anywhere, wooo!
      ;(update :generator ro-gen)
      (update :checker antithesis-checker)
      ))
