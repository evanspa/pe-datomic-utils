(ns user
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer (pprint)]
            [clojure.repl :refer :all]
            [clojure.stacktrace :refer (e)]
            [clj-time.core :as t]
            [clojure.test :as test]
            [datomic.api :as d]
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
  (core/refresh-db datomic-db-uri ["user-schema.dtm"]))
