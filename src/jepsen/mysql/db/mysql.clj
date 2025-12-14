(ns jepsen.mysql.db.mysql
  "Installs MySQL via the official MySQL repositories"
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [lazyfs :as lazyfs]
                    [util :as util :refer [meh]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [jepsen.mysql [client :as mc]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [clj-commons.slingshot :refer [try+ throw+]]))

(def data-dir
  "Where does MySQL store its mutable data files?"
  "/var/lib/mysql")

(defn install!
  "Installs MySQL"
  [test node]
  (c/su
    (c/cd "/tmp"
          ; See https://dev.mysql.com/downloads/repo/apt/
          (let [deb (cu/wget! "https://dev.mysql.com/get/mysql-apt-config_0.8.26-1_all.deb")]
            (c/exec "DEBIAN_FRONTEND=noninteractive" :dpkg :-i deb)))
    (c/exec :apt :update)
    (debian/install [:mysql-server :mysql-client])))

(defn configure!
  "Writes config files. pre-replication? is true when we configure prior to
  setting up replication, and false thereafter."
  [test node pre-replication?]
  (-> (io/resource "my.cnf")
       slurp
       (str/replace #"%IP%" (cn/ip node))
       (str/replace #"%SERVER_ID%" (str (inc (.indexOf (:nodes test) node))))
       ; We include a mariadb header in our config file and delete it for
       ; mysql, so the config options all live under mysql.
       (str/replace #"\[mariadb\]" "")
       ; Does not exist in mysql
       (str/replace #"log-basename.*\n" "")
       ; Deprecated in mysql
       (str/replace #"%BINLOG_FORMAT%"
                    (.toUpperCase (name (:binlog-format test))))
       (str/replace #"%BINLOG_TRANSACTION_DEPENDENCY_TRACKING%"
                    (case (:binlog-transaction-dependency-tracking test)
                      :commit-order "COMMIT_ORDER"
                      :writeset "WRITESET"
                      :writeset-session "WRITESET_SESSION"))
       ; Followers are super-read-only to prevent updates from accidentally
       ; arriving. Note that if we *don't* do this, mysql will murder itself by
       ; trying to run replication transactions at the same time as read
       ; queries and letting the read queries take locks, breaking the
       ; replication update thread entirely? This might be the worst system
       ; I've ever worked on.
       (str/replace #"%SUPER_READ_ONLY%"
                    (if (or pre-replication?
                            (= node (jepsen/primary test)))
                      "OFF"
                      "ON"))
       ; This option lets you totally break serializability by setting it to
       ; OFF. Definitely a thing you, or at least amazon, want in production
       (str/replace #"%REPLICA_PRESERVE_COMMIT_ORDER%"
                    (:replica-preserve-commit-order test))
       (str/replace #"%INNODB_FLUSH_LOG_AT_TRX_COMMIT%"
                    (str (:innodb-flush-log-at-trx-commit test)))
       (cu/write-file! "/etc/mysql/mysql.conf.d/99-jepsen.cnf")))

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
               "GRANT ALL PRIVILEGES ON " mc/db ".* TO " u ";\n"
               ; So we can set up replication
               ; Super, notably not inclusive of reload d=('_`)=b
               "GRANT SUPER ON *.* TO " u ";\n"
               "GRANT RELOAD ON *.* TO " u ";\n"
               "GRANT REPLICATION SLAVE ON *.* to " u ";\n"
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

(defn kill!
  "Kills MySQL"
  []
  (c/su (cu/grepkill! "mysqld")
        (meh (c/exec :systemctl :stop :mysql))))


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

(defrecord DB [lazyfs     ; A LazyFS object
               repl-state ; A promise which will receive the file and position
                          ; of the leader node
                          ]
  db/DB
  (setup! [this test node]
    (install! test node)
    (configure! test node true) ; Pre-replication config
    (when (:lazyfs test)
      (db/setup! lazyfs test node))
    (c/su (c/exec :chown "mysql:mysql" data-dir))
    (c/sudo :mysql (c/exec :mysqld :--initialize-insecure))
    ;(c/su (c/exec :touch "/var/log/mysql/error.log")
    ;      (c/exec :chown "mysql:adm" "/var/log/mysql/error.log"))
    (db/start! this test node)
    (make-db!)
    (jepsen/synchronize test)
    (setup-replication! test node repl-state)
    (jepsen/synchronize test)
    (when (not= node (jepsen/primary test))
      ; Now we take down the secondary and force it to be a super-read-only node
      (c/su (c/exec :systemctl :stop :mysql))
      (configure! test node false)
      (db/start! this test node))

    (when (:lazyfs test)
      (lazyfs/checkpoint! lazyfs)))

  (teardown! [this test node]
    (kill!)
    (when (:lazyfs test)
      (db/teardown! lazyfs test node))
    (c/su
      (c/exec :rm :-rf
              (c/lit (str data-dir "/*"))
              (c/lit "/var/log/mysql/*"))))

  db/LogFiles
  (log-files [this test node]
    (merge {"/var/log/mysql/error.log"              "error.log"
            "/etc/mysql/mysql.conf.d/99-jepsen.cnf" "my-jepsen.cnf"
            (str "/var/lib/mysql/" node ".log")     "query.log"}
           (when (:lazyfs test) (db/log-files lazyfs test node))))

  db/Kill
  (start! [this test node]
    ; lol
    (c/su
      (meh (c/exec :systemctl :start :mysql))
      (c/exec :systemctl :start :mysql)))

  (kill! [this test node]
    (kill!)
    (when (:lazyfs test)
      (lazyfs/lose-unfsynced-writes! lazyfs))))

(defn db
  "A MySQL database. Takes CLI options."
  [opts]
  (map->DB {:repl-state (promise)
            :lazyfs     (lazyfs/db {:dir        data-dir
                                    :cache-size "1GB"})}))
