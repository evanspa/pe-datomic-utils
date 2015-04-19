(ns pe-datomic-utils.test-utils
  (:require [datomic.api :as d]
            [clojure.pprint :refer [pprint]]
            [clojure.java.io :refer [resource]]))

(def postgresql-user "datomic")
(def postgresql-password "datomic")
(def postgresql-db-name "datomic")
(def postgresql-server "localhost")
(def postgresql-port "5432")

(def db-uri
  (str "datomic:sql://"
       "testing" ;db-name
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

;(def db-name-postfix-num (atom 0))

; Because: https://groups.google.com/forum/#!topic/datomic/1WBgM84nKmc
;; (defn db-uri-fn
;;   []
;;   (reset! db-name-postfix-num (inc @db-name-postfix-num))
;;   (let [db-name (str "testing_" @db-name-postfix-num)]
;;     (pprint (str "db-name: " db-name))
;;     db-name))

(def user-schema-files ["user-schema.dtm"])
(def user-partition :user)
