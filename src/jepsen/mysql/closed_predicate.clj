(ns jepsen.mysql.closed-predicate
  "An experimental test for closed predicates."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [dom-top.core :refer [loopr with-retry]]
            [elle [core :as elle]
                  [closed-predicate :as cp]]
            [jepsen
             [checker :as checker]
             [client :as client]
             [core :as jepsen]
             [generator :as gen]
             [history :as h]
             [independent :as independent]
             [store :as store]
             [util :as util :refer []]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle.append :as append]
            [jepsen.mysql [client :as c]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sqlb]
            [slingshot.slingshot :refer [try+ throw+]]))

(def default-table-count 3)

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "txn" i))

(defn table-for
  "What table should we use for the given key?"
  [table-count k]
  (table-name (mod (hash k) table-count)))

(defn all-table-names
  "Every table."
  [table-count]
  (map table-name (range table-count)))

(defn predicate-where
  "Takes a predicate form like [:= 3] and constructs a WHERE clause like
  ['value = ?' 3]"
  [pred]
  (case pred
    :true ["TRUE"]
    (let [[f a b] pred]
      (case f
        := ["value = ?" a]
        :mod ["MOD(value, ?) = ?" a b]))))

(defn mop!
  "Executes a transactional micro-op on a connection. Returns the completed
  micro-op."
  [conn test system txn? [f k v]]
  (Thread/sleep (rand-int 10))
  (let [table-count (:table-count test default-table-count)]
    [f k (case f
           ; Single-key read
           :r (let [table (table-for table-count k)
                    r (j/execute! conn [(str "select value from " table
                                             " where `system` = ? and id = ?")
                                        system k]
                                  {:builder-fn rs/as-unqualified-lower-maps})]
                (:value (first r)))

           ; Predicate read
           :rp (let [tables (shuffle (all-table-names table-count))
                     [where & where-vals] (predicate-where k)
                     rows (mapcat (fn [table]
                                    (j/execute!
                                      conn
                                      (into [(str "select * from "
                                                  table " where `system` = ? and "
                                                  where)
                                             system]
                                            where-vals)
                                      {:builder-fn rs/as-unqualified-lower-maps}))
                                  tables)]
                 ;(info :rp-rows (with-out-str (pprint rows)))
                 (into (sorted-map) (map (juxt :id :value) rows)))

           ; Insert
           :insert (let [table (table-for table-count k)]
                     (j/execute! conn [(str "insert into " table
                                            " (`system`, id, `value`) values (?, ?, ?)")
                                       system k v])
                     v)
           ; Overwrite
           :w (let [table (table-for table-count k)
                    res (j/execute-one! conn [(str "update " table
                                               " set `value` = ? where"
                                               " `system` = ? and id = ?")
                                              v system k])]
                (assert (= 1 (:next.jdbc/update-count res))
                        (str "Expected write of key " (pr-str k) " = " (pr-str v) " to update one row, but it affected " (:next.jdbc/update-count res)))
                v)

           ; Delete
           :delete (let [table (table-for table-count k)
                         res (j/execute-one! conn [(str "delete from " table
                                                        " where `system` = ? and id = ?") system k])]
                     (assert (= 1 (:next.jdbc/update-count res))
                             (str "Expected delete of key " (pr-str k) " = " (pr-str v) " to affect one row, but it deleted " (:next.jdbc/update-count res)))
                     v))]))

; initialized? is an atom which we set when we first use the connection--we set
; up initial isolation levels, logging info, etc. This has to be stateful
; because we don't necessarily know what process is going to use the connection
; at open! time.
(defrecord Client [node conn initialized?]
  client/Client
  (open! [this test node]
    (let [c (c/open test node)]
      (assoc this
             :node          node
             :conn          c
             :initialized?  (atom false))))

  (setup! [_ test]
    (c/with-logging test [conn conn]
    (when (= (jepsen/primary test) node)
        (dotimes [i (:table-count test default-table-count)]
          (j/execute! conn
                      [(str "create table if not exists " (table-name i)
                            " (`system` int not null,
                            id int not null,
                            `value` int not null,
                            primary key (`system`, id))")])
          (j/execute! conn
                      [(str "create index system_idx_" i " on " (table-name i)
                            " (`system`)")])
          ; Make sure we start fresh--in case we're using an existing
          ; cluster and the DB automation isn't wiping the state for us.
          (j/execute! conn [(str "delete from " (table-name i))])))))

  (invoke! [this test {:keys [f value] :as op}]
    (let [[system value] value]
      ; One-time connection setup
      (when (compare-and-set! initialized? false true)
        (c/set-transaction-isolation! conn (:isolation test)))

      (case f
        :init (let [table-count (:table-count test default-table-count)]
                (j/with-transaction [t conn
                                     {:isolation (:isolation test)}]
                  (c/with-logging test [t t]
                    (doseq [[k v] value]
                      (j/execute-one!
                        t [(str "insert into "
                                (table-for table-count k)
                                " (`system`, id, `value`) values (?, ?, ?)")
                           system k v]))))
                (assoc op :type :ok))

        ; await-init is just a :txn, but we don't want to let it be seen by the
        ; checker
        :await-init (-> (client/invoke! this test (assoc op :f :txn))
                        (assoc :f :await-init))

        :txn (c/with-errors op
               (let [txn       value
                     use-txn?  (< 1 (count txn))
                     txn'      (if use-txn?
                                 (j/with-transaction
                                   [t conn {:isolation (:isolation test)}]
                                   (c/with-logging test [t t]
                                     (mapv (partial mop! t test system true)
                                           txn)))
                                 (c/with-logging test [conn conn]
                                   (mapv (partial mop! conn test system false)
                                         txn)))]
                 (assoc op
                        :type :ok
                        :value (independent/tuple system txn')))))))

  (teardown! [_ test])

  (close! [this test]
    (c/close! conn)))

(defn read-only
  "Converts writes to reads."
  [op]
  (loopr [txn' []]
         [[f k v :as mop] (:value op)]
         (recur (conj txn' (case f
                             :r      mop
                             :rp     mop
                             :insert [:r k nil]
                             :delete [:r k nil]
                             :w      [:r k nil])))
         (assoc op :value txn')))

(defn on-follower?
  "Is the given operation going to execute on a follower?"
  [test op]
  (not= 0 (mod (:process op) (count (:nodes test)))))

(defn ro-gen
  "Nothing stops you from writing to a secondary, which is, uh, exciting. We'll
  set up our generator to *only* emit reads to any non-primary node."
  [gen]
  (reify gen/Generator
    (update [this test ctx event]
      (ro-gen (gen/update gen test ctx event)))

    (op [this test ctx]
      (when-let [[op gen'] (gen/op gen test ctx)]
        (cond (= :pending op)
              [:pending this]

              (and (= :txn (:f op)) (on-follower? test op))
              [(read-only op) (ro-gen gen')]

              true
              [op (ro-gen gen')])))))

(defn await-init-gen
  "Takes an init operation and constructs a generator which performs reads of a
  single key in the init set until that key appears."
  [init]
  ; If the init value contains no values, we don't need to wait for anything
  (when-let [[k v] (first (:value init))]
    (reify gen/Generator
      (op [this test context]
        [(gen/fill-in-op {:f :await-init, :value [[:r k nil]]} context)
         this])

      (update [this test context {:keys [f type value] :as event}]
        (info :update event)
        (if (and (= f    :await-init)
                 (= type :ok)
                 (= value [[:r k v]]))
          ; We're done!
          (info "Observed init replication")
          ; Gotta wait
          this)))))

(defn gen
  "Takes CLI options and constructs a generator which emits an init op to the
  primary, then a mix of transactions."
  [opts]
  (delay
    ; Unfurl into a random init op and a lazy seq of txns
    (let [n             (count (:nodes opts))
          [init & txns] (cp/gen opts)]
      ; Construct a replacement generator...
      (gen/phases
        ; Only primary-connected threads can perform the initial write
        (gen/on-threads (fn primary? [thread]
                          (= 0 (mod thread n)))
                        init)
        ; Then wait until every thread sees it. We're implicitly assuming
        ; session consistency here.
        (gen/each-thread (await-init-gen init))
        ; Then do normal transactions.
        (ro-gen txns)))))

(defn checker
  "Uses Elle to analyze predicate safety"
  [opts-]
  (reify checker/Checker
    (check [this test history opts]
      ;(info :checker-opts (with-out-str (pprint opts)))
      (cp/check {:consistency-models [(:expected-consistency-model opts-)]
                 :directory (store/path test (:subdirectory opts))}
                (h/filter-f #{:init :txn} history)))))

(defn sloppy-independent-checker
  "independent/checker is conservative and returns :valid? :unknown if any of
  the subhistories is unknown. Because we generate a ton of histories, it's OK
  if a few are unknown because of an empty transaction graph."
  [checker]
  (reify checker/Checker
    (check [this test history opts]
      (let [res     (checker/check (independent/checker checker)
                                   test history opts)
            n       (count (:results res))
            unknown (filter (fn [res]
                              (= (:valid? res) :unknown))
                            (:results res))

            empty-txn (filter (fn [res]
                                (= (:anomaly-types res)
                                   [:empty-transaction-graph]))
                              unknown)
            _ (info :n n :unknown (count unknown) :empty (count empty-txn))
            valid? (cond ; Any failure makes the whole test fail
                         (= false (:valid res))
                         false

                         ; We're OK with up to 20% empty txn graphs so long as
                         ; ALL of the unknown results are because of empty txn
                         ; graphs
                         (and (= (count unknown) (count empty-txn))
                              (< (/ (count empty-txn) n) 0.2))
                         true

                         ; Otherwise pass through
                         true
                         (:valid? res))]
        (assoc res :valid? valid?)))))

(defn workload
  "A closed-predicate workload"
  [opts]
  {:generator (independent/concurrent-generator
                (* 2 (count (:nodes opts)))
                (range)
                (fn [system]
                  (gen opts)))
   ; Moar concurrency???
   ;:concurrency (* 10 (:concurrency opts))
   :checker (checker/compose
              {:timeline (independent/checker (timeline/html))
               :elle (sloppy-independent-checker (checker opts))})
   :client  (Client. nil nil nil)})
