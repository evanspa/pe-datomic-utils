(ns user
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer (pprint)]
            [clojure.repl :refer :all]
            [clojure.stacktrace :refer (e)]
            [clj-time.core :as t]
            [clojure.test :as test]
            [datomic.api :as d]
            [clj-time.core :as t]
            [pe-datomic-utils.core :as core]
            [clojure.java.io :refer [resource]]
            [clojure.tools.namespace.repl :refer (refresh refresh-all)]))

(def postgresql-user "datomic")
(def postgresql-password "datomic")
(def postgresql-db-name "datomic")
(def postgresql-server "localhost")
(def postgresql-port "5432")

(def datomic-db-name "testing")
(def datomic-db-uri (str "datomic:sql://"
                         datomic-db-name
                         "?jdbc:postgresql://"
                         postgresql-server
                         ":"
                         postgresql-port
                         "/"
                         postgresql-db-name
                         "?user="
                         postgresql-user
                         "&password="
                         postgresql-password))

(defn refresh-db
  []
  (do
    (d/create-database datomic-db-uri)
    (let [conn (d/connect datomic-db-uri)]
      (core/transact-schema-files conn ["user-schema.dtm"])
      conn)))

(defn make-partition
  [conn]
  (core/transact-partition conn :user))

(defn make-user
  [conn]
  (let [user-entid (d/tempid :user)
        future @(d/transact conn
                            [{:db/id user-entid
                              :user/name "Paul"
                              :user/email "paul@example.com"}])]
    (d/resolve-tempid (d/db conn) (:tempids future) user-entid)))

(defn make-user-2
  [conn]
  (let [user-entid (d/tempid :user)
        future @(d/transact conn
                            [{:db/id user-entid
                              :user/name "Dave"
                              :user/email "Dave@example.com"}])]
    (d/resolve-tempid (d/db conn) (:tempids future) user-entid)))

(defn make-order
  [conn user-entid name]
  (let [order-entid (d/tempid :user)
        future @(d/transact conn
                            [{:db/id order-entid
                              :order/user user-entid
                              :order/name name}])]
    (d/resolve-tempid (d/db conn) (:tempids future) order-entid)))

(defn pprint-q-results
  [conn result-set]
  (pprint (reduce (fn [results relation]
                    (let [entid (first relation)]
                      (conj results
                            (into {:db/id entid} (d/entity (d/db conn) entid)))))
                  []
                  result-set)))

(defn display-users
  [conn]
  (pprint-q-results conn
                    (d/q '[:find ?usr :where [?usr :user/name _]]
                         (d/db conn))))

(defn pprint-tx-range
  [conn start]
  (pprint (reduce (fn [txns txn]
                    (conj txns txn))
                  []
                  (d/tx-range (d/log conn) start nil))))

(defn retract-user
  [conn user-entid]
  @(d/transact conn [[:db.fn/retractEntity user-entid]]))
