(ns jepsen.mysql.db.maria
  "Automates setting up and tearing down MariaDB, replicated using Galera
  Cluster."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util :refer [meh await-fn]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [jepsen.mysql [client :as mc]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [org.httpkit.client :as http]
            [clj-commons.slingshot :refer [try+ throw+]]))

(defn configure-repo!
  "Sets up the MariaDB repo on the current node."
  [test]
  (c/su
    ; There's also
    ; https://mariadb.org/download/?t=repo-config&d=Debian+Linux&v=12.1&r_m=acorn,
    ; but the apt sources files it generates are useless; they all lack a
    ; Release file.
    (let [script (cu/cached-wget! "https://r.mariadb.com/downloads/mariadb_repo_setup")]
      (c/exec :chmod "+x" script)
      (c/exec script)
      ; This is going to leave backup files that apt will complain about
      (c/exec :rm :-f (c/lit "/etc/apt/sources.list.d/mariadb.list.old*")))
    ;(debian/update!)
    ))

(defn install!
  "Installs MariaDB"
  [test node]
  (configure-repo! test)
  (c/su
    (debian/install ["mariadb-server" "galera-4"])
    ; Work around an LXC bug
    (c/exec :mkdir :-p "/etc/systemd/system/mariadb.service.d")
    (cu/write-file! "[Service]
ProtectHome=false
ProtectSystem=false
PrivateDevices=false"
                    "/etc/systemd/system/mariadb.service.d/lxc.conf")
    (c/exec :systemctl :daemon-reload)))

(defn cluster-address
  "The gcomm:// Galera cluster address used in configuring replicas. Per
  https://mariadb.com/docs/galera-cluster/galera-management/configuration/galera-cluster-address#cluster-address,
  this should be all nodes in the cluster. They *say* you can include the
  current node, but I think, based on suspicious 'blacklisting address'
  messages in the logs, they might be wrong about that."
  [test node]
  (str "gcomm://"
       (str/join "," (remove #{node} (:nodes test)))))

(defn configure!
  "Writes config files"
  [test node]
  (-> (io/resource "my.cnf")
       slurp
       (str/replace #"%IP%" (cn/ip node))
       (str/replace #"%SERVER_ID%" (str (inc (.indexOf (:nodes test) node))))
       (str/replace #"%REPLICA_PRESERVE_COMMIT_ORDER%" (:replica-preserve-commit-order test))
       (str/replace #"%BINLOG_FORMAT%"
                    (.toUpperCase (name (:binlog-format test))))
       (str/replace #"%BINLOG_TRANSACTION_DEPENDENCY_TRACKING%"
                    (case (:binlog-transaction-dependency-tracking test)
                      :commit-order "COMMIT_ORDER"
                      :writeset "WRITESET"
                      :writeset-session "WRITESET_SESSION"))
       (str/replace #"%INNODB_FLUSH_LOG_AT_TRX_COMMIT%"
                    (str (:innodb-flush-log-at-trx-commit test)))
       (cu/write-file! "/etc/mysql/mariadb.conf.d/90-jepsen.cnf"))
  ; And Galera file
  (-> (io/resource "galera.cnf")
      slurp
      (str/replace #"%INNODB_FLUSH_LOG_AT_TRX_COMMIT%"
                    (str (:innodb-flush-log-at-trx-commit test)))
      (str/replace #"%CLUSTER_ADDRESS%"
                   (cluster-address test node))
      (cu/write-file! "/etc/mysql/mariadb.conf.d/99-jepsen-galera.cnf")))

(defn sql!
  "Evaluates mysql with the given SQL string as root."
  [sql]
  (let [action {:cmd "mariadb -u root"
                :in  sql}]
    (c/su
      (-> action
          c/wrap-cd
          c/wrap-sudo
          c/wrap-trace
          c/ssh*
          c/throw-on-nonzero-exit))))

(defn make-db!*
  "Adds a user and DB with remote access."
  [test]
  (info "Making DB")
  (try+
    (let [u (str "'" mc/user "'@'%'")]
      (sql! (str "CREATE DATABASE " mc/db ";\n"
                 "CREATE USER " u " IDENTIFIED BY '" mc/password "';\n"
                 ; So we can set up replication
                 ;"GRANT RELOAD ON *.* TO " u ";\n"
                 ;"GRANT BINLOG MONITOR ON *.* to " u ";\n"
                 ;"GRANT SLAVE MONITOR ON *.* to " u ";\n"
                 ;"GRANT REPLICATION SLAVE ADMIN ON *.* to " u ";\n"
                 ;"GRANT REPLICATION SLAVE ON *.* to " u ";\n"
                 "GRANT ALL PRIVILEGES ON " mc/db ".* TO " u ";\n"
                 "FLUSH PRIVILEGES;\n"
                 ";\n")))
    (catch [:type :jepsen.control/nonzero-exit] e
      (condp re-find (:err e)
        #"database exists" nil
        (throw+ e)))))

(defn make-db!
  "Like make-db*!, but retries when things aren't ready."
  [test]
  (await-fn (partial make-db!* test)
            {:log-interval 10000
             :log-message "Waiting to create database"
             :retry-interval 5000
             :timeout 300000}))

(defn await-slave-sql-running
  "Run on the secondary to block until slave-sql-running and slave-io-running
  are true."
  [conn]
  (util/await-fn (fn attempt []
                   (let [r (j/execute-one! conn ["SHOW SLAVE STATUS"])]
                     ;(info :r (with-out-str (pprint r)))
                     (when-not (= "Yes"
                                  (:Slave_IO_Running r)
                                  (:Slave_SQL_Running r))
                       (throw+ {:type :slave-not-running
                                :status r}))))
                 {:retry-interval 1000
                  :log-interval   10000
                  :log-message    "Waiting for slave IO to start running"
                  :timeout        60000}))

(defn setup-replication!
  "Initiates binlog replication between the primary and secondaries."
  [test node repl-state]
  ; Following https://mariadb.com/kb/en/setting-up-replication/
  (let [c (mc/await-open test node)]
    (if (= (jepsen/primary test) node)
      ; On leader
      (do (info "Setting up leader for replication")
          (j/execute! c ["FLUSH TABLES WITH READ LOCK"])
          (let [r (j/execute-one! c ["SHOW MASTER STATUS"])
                pos (:Position r)
                file (:File r)]
            ;(info :pos pos, :file file)
            (deliver repl-state {:position pos, :file file})
            ; TODO: copy data here
          (j/execute! c ["UNLOCK TABLES"])))
      ; On followers
      (let [{:keys [position file]} @repl-state]
        (info "Setting up follower for replication")
        (j/execute-one! c [(str "CHANGE MASTER TO"
                                " MASTER_HOST='" (jepsen/primary test)
                                "', MASTER_USER='" mc/user
                                "', MASTER_PASSWORD='" mc/password
                                "', MASTER_PORT=" mc/port
                                ", MASTER_LOG_FILE='" file
                                "', MASTER_LOG_POS=" position
                                ; lmao what, why, docs say to do this but ????
                                ", MASTER_CONNECT_RETRY=10;")])
        (j/execute-one! c ["START SLAVE"])
        (await-slave-sql-running c)))))

(defn bootstrap-primary!
  "On the primary node only, run the bootstrap phase, as per https://mariadb.com/docs/galera-cluster/galera-management/installation-and-deployment/getting-started-with-mariadb-galera-cluster."
  [test node]
  (when (= (jepsen/primary test) node)
    (c/su
      (info "Bootstrapping first node")
      ; This will sometimes explode if it doesn't find an existing
      ; /run/mysql or /run/mysqld. Fun!
      (c/exec :mkdir :-p "/run/mysql")
      (c/exec :mkdir :-p "/run/mysqld")
      (c/exec :chown "mysql:mysql" "/run/mysql")
      (c/exec :chown "mysql:mysql" "/run/mysqld")
      (c/exec :galera_new_cluster))))

(defn db
  "A MySQL database. Takes CLI options."
  [opts]
  (let [; A promise which will receive the file and position of the leader node
        repl-state (promise)]
    (reify db/DB
      (setup! [this test node]
        ; Install
        (install! test node)
        (configure! test node)

        ; Create internal tables
        (info "Install DB")
        (c/sudo :mysql (c/exec :mariadb-install-db))

        ; Bootstrap
        (bootstrap-primary! test node)
        (jepsen/synchronize test)

        ; Add remaining nodes
        (db/start! this test node)

        ; And create our DB
        (make-db! test)
        ;(jepsen/synchronize test)
        ;(setup-replication! test node repl-state)
        )

      (teardown! [this test node]
        (db/kill! this test node)
        (c/su
          (c/exec :rm :-rf
                  (c/lit "/var/lib/mysql/*")
                  (c/lit "/var/log/mysql/*"))))

      db/LogFiles
      (log-files [this test node]
        {"/var/lib/mysql/error.log"                       "error.log"
         "/var/lib/mysql/general.log"                     "general.log"
         "/etc/mysql/mariadb.conf.d/99-jepsen-galera.cnf" "jepsen-galera.cnf"
         "/etc/mysql/mariadb.conf.d/90-jepsen.cnf"        "jepsen.cnf"})

      db/Kill
      (start! [this test node]
        (c/su (c/exec :systemctl :start :mariadb)))

      (kill! [this test node]
        (c/su (cu/grepkill! "mariadbd")
              (meh (c/exec :systemctl :stop :mariadb)))))))
