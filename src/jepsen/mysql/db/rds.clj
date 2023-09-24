(ns jepsen.mysql.db.rds
  "A MySQL database backed by AWS RDS."
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
            [slingshot.slingshot :refer [try+ throw+]]))

(defn aws!
  "Runs an AWS shell command, returning the results as a parsed JSON structure."
  [& args]
  (let [{:keys [exit out] :as res} (apply sh "aws" args)]
    (when (not= 0 exit)
      (throw+ (assoc res :type :aws-command-failed)))
    (json/parse-string out)))

(defn db
  "Takes CLI options and returns a database backed by AWS RDS. Shells out to
  AWS CLI."
  [opts]
  (let [; A promise which will receive the file and position of the leader node
        repl-state (promise)]
    (reify db/DB
      (setup! [this test node]
        (info (aws! "rds" "describe-db-clusters")))

      (teardown! [this test node]))))
