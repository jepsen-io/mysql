(ns jepsen.mysql.repl
  (:require [dom-top.core :refer [loopr]]
            [jepsen [history :as h]
                    [store :as store]]
            [elle [txn :as txn]
                  [list-append :as append]]))

(defn suspicious
  "A map of keys to vectors of all elements read but not in the longest
  version."
  [test]
  (let [incompatible-orders (-> test :results :workload :anomalies :incompatible-order)
        history (h/client-ops (:history test))
        sorted-values (append/sorted-values history)]
    (->> incompatible-orders
         (map :key)
         set
         (map (fn [k]
                (let [values  (sorted-values k)
                      longest (set (last values))]
                  [k (loopr [sus (sorted-set)]
                            [value   values
                             element value]
                            (if (longest element)
                              (recur sus)
                              (recur (conj sus element))))]))))))

(defn write-types
  "Takes a map of keys to elements. Turns those elements into [type element]
  pairs, where the type is the :type of the operation that appended the
  element."
  [test sus]
  (let [wi (append/write-index (h/client-ops (:history test)))]
    (loopr [m {}]
           [[k vs] sus
            v      vs]
           (recur
             (assoc m k
                    (conj (get m k [])
                          [v (-> wi (get k) (get v) :type)]))))))
