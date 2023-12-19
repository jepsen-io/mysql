(ns jepsen.mysql.mav
  "An experimental test for monotonic atomic view. Writers change the values of
  two different rows A and B. Readers read A, write to B, read B, then read A
  again."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [dom-top.core :refer [loopr with-retry]]
            [jepsen
             [checker :as checker]
             [client :as client]
             [core :as jepsen]
             [generator :as gen]
             [history :as h]
             [store :as store]
             [util :as util :refer []]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.mysql [client :as c]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sqlb]
            [slingshot.slingshot :refer [try+ throw+]]))

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
             :conn          c)))

  (setup! [_ test]
    (c/with-logging test [conn conn]
      (when (= (jepsen/primary test) node)
        (when (compare-and-set! initialized? false true)
          (j/execute! conn
                      [(str "create table if not exists mav"
                            " (`id`    int not null,
                               `value` int not null,
                               noop    int not null,
                               primary key (id))")])
          ; Make sure we start fresh--in case we're using an existing
          ; cluster and the DB automation isn't wiping the state for us.
          (j/execute! conn [(str "delete from mav")])
          (j/execute! conn ["insert into mav (id, `value`, noop) values (?, ?, ?)" 0 0 0])
          (j/execute! conn ["insert into mav (id, `value`, noop) values (?, ?, ?)" 1 0 0])))))

  (invoke! [this test {:keys [f value] :as op}]
    (case f
      :write
      (j/with-transaction [t conn {:isolation (:isolation test)}]
        (c/with-logging test [t t]
          (doseq [id (shuffle [0 1])]
            (Thread/sleep (long (rand-int 10)))
            (j/execute! t ["update mav set value = value + 1 where id = ?"
                           id]))
          (assoc op :type :ok)))

      :read
      (j/with-transaction [t conn {:isolation (:isolation test)}]
        (c/with-logging test [t t]
          (let [{:keys [id]} value
                a1 (-> t
                       (j/execute-one! ["select value from mav where id = ?" 0])
                       :mav/value)
                _ (Thread/sleep (long (rand-int 10)))
                r (j/execute! t ["update mav set noop = ? where id = ?"
                                 (rand-int 100) 1])
                b2 (-> t
                       (j/execute-one! ["select value from mav where id = ?" 1])
                       :mav/value)
                _ (Thread/sleep (long (rand-int 10)))
                a2 (-> t
                       (j/execute-one! ["select value from mav where id = ?" 0])
                       :mav/value)]
            (assoc op :type :ok, :value {:a1 a1
                                         :b2 b2
                                         :a2 a2}))))))

  (teardown! [_ test])

  (close! [this test]
    (c/close! conn)))

(def write-gen
  "Constant increments of both keys"
  (repeat {:f :write}))

(def read-gen
  "Reads (with a no-op write to mess up the snapshot)"
  (repeat {:f :read}))

(defn checker
  "Looks for instances where reads are nonmonotonic."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [errs (->> history
                      h/oks
                      (h/filter-f :read)
                      (h/filter (fn [op]
                                  (let [{:keys [a1 b2 a2]} (:value op)]
                                    (not (<= a1 b2 a2)))))
                      (into []))]
        {:valid? (not (seq errs))
         :errs   (when (seq errs) errs)}))))

(defn workload
  "A workload which mixes updates of names with reads of names and updates to
  gender."
  [opts]
  {:generator (gen/mix [write-gen read-gen])
   :checker (checker)
   :client  (Client. nil nil (atom false))})
