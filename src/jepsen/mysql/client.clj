(ns jepsen.mysql.client
  "Helper functions for interacting with MySQL clients."
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]
                    [util :as util]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sqlb]
            [clj-commons.slingshot :refer [try+ throw+]])
  (:import (java.sql Connection
                     SQLNonTransientConnectionException
                     SQLTransactionRollbackException)
           (com.mysql.cj.jdbc.exceptions MySQLTransactionRollbackException)))

(def user "jepsen")
(def password "jepsenpw")
(def port 3306)
(def db "jepsen")

(defmacro with-logging
  "Takes a test, a binding vector of [logging-conn-name jdbc-connection], and a
  body. Evaluates body. If the test has (:log-sql test) set, binds conn-name
  within body scope to log SQL statements to the console."
  [test [conn-name conn] & body]
  `(let [~conn-name (if (:log-sql ~test)
                      (j/with-logging ~conn (fn ~'log [op# sql#]
                                              (info op# (pr-str sql#))))
                      ~conn)]
     ~@body))

(defn open
  "Opens a connection to the given node. Options

    :db If nil, skips setting the DB name. Helpful for initial setup."
  ([test node]
   (open test node {}))
  ([test node opts]
   (let [spec  {;:dbtype    "mysql"
                :dbtype    "mariadb"
                :host      node
                :port      port
                :user      user
                :password  password
                "allowPublicKeyRetrieval" true}
         spec  (if (contains? opts :db)
                 (let [db (:db opts)]
                   (if (nil? db)
                     spec
                     (assoc spec :dbname db)))
                 ; Use default
                 (assoc spec :dbname db))
         spec  (if-let [pt (:prepare-threshold test)]
                 (assoc spec :prepareThreshold pt)
                 spec)
         ; Can't do this here; logging objects can't be used for txns
         ;spec  (if (:log-sql test)
         ;        (j/with-logging spec
         ;          (fn log [op sql]
         ;            (info op (pr-str sql))))
         ;        spec)
         ds    (j/get-datasource spec)
         conn  (j/get-connection ds)]
     conn)))

(defn set-transaction-isolation!
  "Sets the transaction isolation level on a connection. Returns conn."
  [conn level]
  (.setTransactionIsolation
    conn
    (case level
      :serializable     Connection/TRANSACTION_SERIALIZABLE
      :repeatable-read  Connection/TRANSACTION_REPEATABLE_READ
      :read-committed   Connection/TRANSACTION_READ_COMMITTED
      :read-uncommitted Connection/TRANSACTION_READ_UNCOMMITTED))
  conn)

(defn close!
  "Closes a connection."
  [^java.sql.Connection conn]
  (.close conn))

(defn await-open
  "Waits for a connection to node to become available, returning conn. Helpful
  for starting up."
  [test node]
  (util/await-fn (fn attempt []
                   (let [conn (open test node)]
                     (try (j/execute-one! conn
                                          ["create table if not exists jepsen_await (id int)"])
                          conn
                          ; TODO: catch duplicate table and also return conn
                          )))
                 {:retry-interval 1000
                  :log-interval   60000
                  :log-message    "Waiting for MySQL connection"
                  :timeout        10000}))

(defmacro with-errors
  "Takes an operation and a body, turning known errors into :fail or :info ops."
  [op & body]
  `(try+ ~@body
        (catch SQLTransactionRollbackException e#
          (assoc ~op :type :fail, :error :rollback))
        (catch MySQLTransactionRollbackException e#
          (assoc ~op :type :fail, :error :rollback))
        (catch (and (:rollback ~'%) (:handling ~'%)) e#
          (condp re-find (:message ~'&throw-context)
            #"Rollback failed"
            (assoc ~op :type :info, :error :rollback-failed)

            (throw+ e#)))
        (catch SQLNonTransientConnectionException e#
          (condp re-find (.getMessage e#)
            #"Socket error"
            (condp re-find (.getMessage (.getCause e#))
              #"unexpected end of stream"
              (assoc ~op :type :info, :error :unexpected-end-of-stream)

              #"Connection is closed"
              (assoc ~op :type :info, :error :connection-closed)

              #"Connection reset"
              (assoc ~op :type :info, :error :connection-reset)

              (throw+ e#))

            #"WSREP has not yet prepared node for application use"
            (assoc ~op :type :fail, :error :wsrep-not-yet-prepared)

            (throw+ e#)))
        (catch java.sql.SQLException e#
          (condp re-find (.getMessage e#)
            #"Record has changed since last read"
            (assoc ~op :type :fail, :error :record-changed-since-last-read)

            #"Lock wait timeout exceeded"
            (assoc ~op :type :fail, :error :lock-wait-timeout-exceeded)

            #"Not connected to Primary"
            (assoc ~op :type :info, :error :not-connected-to-primary)

            (throw+ e#)))))
