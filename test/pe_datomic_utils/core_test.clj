(ns pe-datomic-utils.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [pe-datomic-utils.core :as core]
            [pe-datomic-utils.test-utils :refer [user-schema-files
                                                 db-uri
                                                 user-partition]]
            [pe-core-utils.core :as ucore]
            [clojure.java.io :refer [resource]]
            [datomic.api :as d]
            [clojure.tools.logging :as log]
            [clj-time.core :as t])
  (:require [datomic.api :as d]
            [clojure.java.io :refer [resource]]))

(def conn (atom nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Helpers
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn make-db-refresher-fixture-fn
  [db-uri conn partition schema-files]
  (fn [f]
    (reset! conn
            (do
              (d/delete-database db-uri)
              (Thread/sleep 60000) ; wait 60 seconds
              (d/create-database db-uri)
              (let [conn (d/connect db-uri)]
                (core/transact-schema-files conn schema-files)
                conn)))
    (core/transact-partition @conn partition)
    (f)))

(defn- user-txnmap
  [user-entid user]
  (merge {:db/id user-entid} user))

(defn save-user-txnmap
  [user-entid user]
  (user-txnmap user-entid user))

(defn save-new-user-txnmap
  [partition user]
  (save-user-txnmap (d/tempid partition) user))

(defn- order-txnmap
  [user-entid order-entid order]
  (merge {:db/id order-entid
          :order/user user-entid}
         order))

(defn save-order-txnmap
  [user-entid order-entid order]
  (order-txnmap user-entid order-entid order))

(defn save-new-order-txnmap
  [partition user-entid order]
  (save-order-txnmap user-entid (d/tempid partition) order))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Fixtures
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(use-fixtures :each (make-db-refresher-fixture-fn db-uri
                                                  conn
                                                  user-partition
                                                  user-schema-files))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest Change-Log
  (testing "change-log functionality"
    #_(let [t1 (.toDate (t/now))
          cl (core/change-log @conn t1 :user/email "paul@ex.com" identity identity)
          updates (:updates cl)
          dels (:deletions cl)]
      (is (= 0 (count updates)))
      (let [p-entid (core/save-new-entity @conn
                                          (save-new-user-txnmap user-partition
                                                                {:user/name "Paul"
                                                                 :user/email "paul@ex.com"}))
            d-entid (core/save-new-entity @conn
                                          (save-new-user-txnmap user-partition
                                                                {:user/name "Dave"
                                                                 :user/email "dave@ex.com"}))
            cl (core/change-log @conn t1 :user/email "paul@ex.com" identity identity)]
        (is (= 0 (count (:deletions cl))))
        (is (= 1 (count (:updates cl))))
        (let [cl (core/change-log @conn t1 :user/email "dave@ex.com" identity identity)
              cl-2 (core/change-log @conn t1 :order/user p-entid identity identity)]
          (is (= 0 (count (:deletions cl))))
          (is (= 1 (count (:updates cl))))
          (is (= 0 (count (:deletions cl-2))))
          (is (= 0 (count (:updates cl-2))))
          (let [p-order-1 (core/save-new-entity @conn
                                                (save-new-order-txnmap user-partition
                                                                       p-entid
                                                                       {:order/user p-entid
                                                                        :order/name "Paul's order 1"}))]
            (let [cl (core/change-log @conn t1 :order/user p-entid identity identity)]
              (is (= 0 (count (:deletions cl))))
              (is (= 1 (count (:updates cl))))
              @(d/transact @conn [{:db/id p-order-1
                                   :order/name "Paul's updated order 1"}])
              (let [cl (core/change-log @conn t1 :order/user p-entid identity identity)]
                (is (= 0 (count (:deletions cl))))
                (is (= 1 (count (:updates cl))))
                @(d/transact @conn [[:db.fn/retractEntity p-order-1]])
                (let [cl (core/change-log @conn t1 :order/user p-entid identity identity)]
                  (is (= 1 (count (:deletions cl))))
                  (is (= 0 (count (:updates cl)))))))))))))
