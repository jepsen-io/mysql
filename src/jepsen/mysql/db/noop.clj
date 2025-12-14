(ns jepsen.mysql.db.noop
  "Connects to an existing MySQL instance. Creates a database, but that's it."
  (:require [cheshire.core :as json]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java [io :as io]
                          [shell :refer [sh]]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util :refer [meh]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [jepsen.mysql [client :as mc]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [clj-commons.slingshot :refer [try+ throw+]]))

(defn db
  "Takes CLI options and returns a database."
  [opts]
  (reify db/DB
    (setup! [this test node]
      (when (= (jepsen/primary test) node)
        (with-open [c (mc/open test node {:db nil})]
          (j/execute-one! c [(str "CREATE DATABASE " mc/db ";\n")])
          ; I think there's a race condition where this horribly breaks, like,
          ; all RDS replication altogether
          (Thread/sleep 10000))))

    (teardown! [this test node]
      (when (= (jepsen/primary test) node)
        (with-open [c (mc/open test node {:db nil})]
          (j/execute-one! c [(str "DROP DATABASE IF EXISTS " mc/db ";\n")]))
        ; I think there's a race condition where this horribly breaks, like,
        ; all RDS replication altogether
        (Thread/sleep 10000)))))
