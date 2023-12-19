(ns jepsen.mysql.db.maria
  "Automates setting up and tearing down MariaDB."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
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

(defn install!
  "Installs MyS^H^HariaDB"
  [test node]
  (c/su
    (debian/install [:mariadb-server])
    ; Work around an LXC bug
    (c/exec :mkdir :-p "/etc/systemd/system/mariadb.service.d")
    (cu/write-file! "[Service]
ProtectHome=false
ProtectSystem=false
PrivateDevices=false"
                    "/etc/systemd/system/mariadb.service.d/lxc.conf")
    (c/exec :systemctl :daemon-reload)))

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
       ; This option doesn't exist in Maria AFAICT
       (str/replace #"replica-preserve-commit-order.*?\n" "")
       ; Followers are super-read-only to prevent updates from
       ; accidentally arriving. Note that if we *don't* do this, mysql
       ; will murder itself by trying to run replication transactions at
       ; the same time as read queries and letting the read queries take
       ; locks, breaking the replication update thread entirely? This
       ; might be the worst system
       ; I've ever worked on.
       (str/replace #".*%SUPER_READ_ONLY%.*" "")
       (str/replace #"%INNODB_FLUSH_LOG_AT_TRX_COMMIT%"
                    (str (:innodb-flush-log-at-trx-commit test)))
       (cu/write-file! "/etc/mysql/mariadb.conf.d/99-jepsen.cnf")))

(defn sql!
  "Evaluates mysql with the given SQL string as root."
  [sql]
  (let [action {:cmd "mysql -u root"
                :in  sql}]
    (c/su
      (-> action
          c/wrap-cd
          c/wrap-sudo
          c/wrap-trace
          c/ssh*
          c/throw-on-nonzero-exit))))

(defn make-db!
  "Adds a user and DB with remote access"
  []
  (let [u (str "'" mc/user "'@'%'")]
    (sql! (str "CREATE DATABASE " mc/db ";\n"
               "CREATE USER " u " IDENTIFIED BY '" mc/password "';\n"
               ; So we can set up replication
               "GRANT RELOAD ON *.* TO " u ";\n"
               "GRANT BINLOG MONITOR ON *.* to " u ";\n"
               "GRANT SLAVE MONITOR ON *.* to " u ";\n"
               "GRANT REPLICATION SLAVE ADMIN ON *.* to " u ";\n"
               "GRANT REPLICATION SLAVE ON *.* to " u ";\n"
               "GRANT ALL PRIVILEGES ON " mc/db ".* TO " u ";\n"
               "FLUSH PRIVILEGES;\n"))))

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
      (do (j/execute! c ["FLUSH TABLES WITH READ LOCK"])
          (let [r (j/execute-one! c ["SHOW MASTER STATUS"])
                pos (:Position r)
                file (:File r)]
            ;(info :pos pos, :file file)
            (deliver repl-state {:position pos, :file file})
            ; TODO: copy data here
          (j/execute! c ["UNLOCK TABLES"])))
      ; On followers
      (let [{:keys [position file]} @repl-state]
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

(defn db
  "A MySQL database. Takes CLI options."
  [opts]
  (let [; A promise which will receive the file and position of the leader node
        repl-state (promise)]
    (reify db/DB
      (setup! [this test node]
        (install! test node)
        (configure! test node)
        (c/sudo :mysql (c/exec :mysql_install_db))
        (db/start! this test node)
        (make-db!)
        (jepsen/synchronize test)
        (setup-replication! test node repl-state))

      (teardown! [this test node]
        (db/kill! this test node)
        (c/su
          (c/exec :rm :-rf
                  (c/lit "/var/lib/mysql/*")
                  (c/lit "/var/log/mysql/*"))))

      db/LogFiles
      (log-files [this test node]
        ["/var/log/mysql/error.log"])

      db/Kill
      (start! [this test node]
        (c/su (c/exec :systemctl :start :mariadb)))

      (kill! [this test node]
        (c/su (cu/grepkill! "mariadbd")
              (meh (c/exec :systemctl :stop :mariadb)))))))
