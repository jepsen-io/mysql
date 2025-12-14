(ns jepsen.mysql.nonrepeatable-read
  "An experimental test for nonrepeatable reads. Some transactions change the
  value of a single row, while another tries to read, perform an idempotent
  write, and read it again."
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
            [clj-commons.slingshot :refer [try+ throw+]]))

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
                      [(str "create table if not exists people"
                            " (`id` int not null,
                            name   text not null,
                            gender text not null,
                            primary key (id))")])
          ; Make sure we start fresh--in case we're using an existing
          ; cluster and the DB automation isn't wiping the state for us.
          (j/execute! conn [(str "delete from people")])
          (j/execute! conn ["insert into people (id, name, gender) values (?, ?, ?)"
                            0 "moss" "enby"])))))

  (invoke! [this test {:keys [f value] :as op}]
    (case f
      :change-name (do (j/execute!
                         conn ["update people set name = ? where id = ?"
                               (:name value) (:id value)])
                       (assoc op :type :ok))

      :read
      (j/with-transaction [t conn {:isolation (:isolation test)}]
        (c/with-logging test [t t]
          (let [{:keys [id]} value
                name1 (-> t
                          (j/execute-one! ["select name from people where id = ?" id])
                          :people/name)
                _ (Thread/sleep (long (rand-int 10)))
                r (j/execute! t ["update people set gender = ? where id = ?"
                                 (:gender value)
                                 id])

                _ (Thread/sleep (long (rand-int 10)))
                name2 (-> t
                          (j/execute-one! ["select name from people where id = ?" id])
                          :people/name)]
            (assoc op :type :ok, :value {:id    id
                                         :name1 name1
                                         :name2 name2}))))

      :delete
      (do (j/execute! conn
                      ["delete from people where id = ?"
                       (:id value)])
          (assoc op :type :ok))

      :insert
      (do (j/execute! conn
                      ["insert into people (id, name, gender) values (?, ?, ?)"
                       (:id value) (:name value) (:gender value)])
          (assoc op :type :ok))))

  (teardown! [_ test])

  (close! [this test]
    (c/close! conn)))

(def name-change-gen
  "Constant name changes"
  (->> ["currant" "leaf" "brick" "truck" "pebble" "moss" "germ" "ferris"
        "s'more"]
       cycle
       (map (fn [name]
              {:f :change-name :value {:id 0, :name name}}))))

(def read-gen
  "Operations which read name, change gender, and read name again."
  (->> ["butch" "dyke" "femme" "enby" "leatherboy" "agender"]
       cycle
       (map (fn [gender]
              {:f :read :value {:id 0, :gender gender}}))))

(def delete-insert-gen
  "Continually deletes and re-creates row 0."
  (cycle
    [{:f :delete, :value {:id 0}}
     {:f :insert, :value {:id 0, :name "moss", :gender "enby"}}]))

(defn checker
  "Looks for instances where name1 and name2 don't agree"
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [errs (->> history
                      h/oks
                      (h/filter-f :read)
                      (h/filter (fn [op]
                                  (let [{:keys [id name1 name2]} (:value op)]
                                    (not= name1 name2)))))]
        {:valid? (not (seq errs))
         :errs   (when (seq errs) (into [] errs))}))))

(defn workload
  "A workload which mixes updates of names with reads of names and updates to
  gender."
  [opts]
  {:generator (gen/mix [name-change-gen delete-insert-gen read-gen])
   :checker (checker)
   :client  (Client. nil nil (atom false))})
