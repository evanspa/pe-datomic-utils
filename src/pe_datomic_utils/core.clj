(ns pe-datomic-utils.core
  (:require [datomic.api :refer [q db] :as d]
            [clj-time.core :as t]
            [clojure.tools.logging :as log]))

(defn txn-time
  "Returns the transaction time (instance) of the given entity e.  attr needs to
   be any attribute of the entity."
  [conn e attr]
  (ffirst (q '[:find ?tx-time
               :in $ ?e ?a
               :where
               [$ ?e ?a _ ?tx]
               [$ ?tx :db/txInstant ?tx-time]]
             (db conn)
             e
             attr)))

(defn save-new-entity
  [conn txnmap]
  (let [tx @(d/transact conn [txnmap])]
    (d/resolve-tempid (d/db conn) (:tempids tx) (:db/id txnmap))))
