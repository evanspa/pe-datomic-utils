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
#_(deftest Is-Entity-Updated-Since
  (testing "is-entity-updated-since"
    (is (not (core/is-entity-updated-since @conn (.toDate (t/now)) 2091234)))
    (let [t1 (.toDate (t/now))
          p-entid (core/save-new-entity @conn
                                        (save-new-user-txnmap user-partition
                                                              {:user/name "Paul"
                                                               :user/email "paul@ex.com"}))
          t2 (.toDate (t/now))]
      (is (core/is-entity-updated-since @conn t1 p-entid))
      (is (not (core/is-entity-updated-since @conn t2 p-entid)))
      @(d/transact @conn [{:db/id p-entid :user/name "Paully"}])
      (let [t3 (.toDate (t/now))]
        (is (core/is-entity-updated-since @conn t1 p-entid))
        (is (core/is-entity-updated-since @conn t2 p-entid))
        (is (not (core/is-entity-updated-since @conn t3 p-entid)))
        @(d/transact @conn [{:db/id p-entid :user/name "Paully"}])
        (let [t4 (.toDate (t/now))]
          (is (core/is-entity-updated-since @conn t1 p-entid))
          (is (core/is-entity-updated-since @conn t2 p-entid))
          ; should still be 'no' after t3 because previous update made no
          ; change to the value of our entity
          (is (not (core/is-entity-updated-since @conn t3 p-entid)))
          (is (not (core/is-entity-updated-since @conn t4 p-entid)))
          @(d/transact @conn [{:db/id p-entid :user/name "Puh"}])
          (let [t5 (.toDate (t/now))]
            (is (core/is-entity-updated-since @conn t4 p-entid))
            (is (not (core/is-entity-updated-since @conn t5 p-entid)))
            @(d/transact @conn [{:db/id p-entid :user/name "Paul"}])
            (let [t6 (.toDate (t/now))]
              (is (core/is-entity-updated-since @conn t5 p-entid))
              (is (not (core/is-entity-updated-since @conn t6 p-entid))))))))))

#_(deftest Is-Entity-Deleted-Since
  (testing "is-entity-deleted-since"
    (is (not (core/is-entity-deleted-since @conn (.toDate (t/now)) 2091234)))
    (let [t1 (.toDate (t/now))
          p-entid (core/save-new-entity @conn
                                        (save-new-user-txnmap user-partition
                                                              {:user/name "Paul"
                                                               :user/email "paul@ex.com"}))
          t2 (.toDate (t/now))]
      (is (not (core/is-entity-deleted-since @conn t1 p-entid)))
      @(d/transact @conn [[:db.fn/retractEntity p-entid]])
      (let [t3 (.toDate (t/now))]
        (is (core/is-entity-deleted-since @conn t2 p-entid))
        (is (not (core/is-entity-deleted-since @conn t3 p-entid)))))))

#_(deftest Change-Log-Since
  (testing "change-log-since functionality"
    (let [t1 (.toDate (t/now))
          cl (core/change-log-since @conn t1 :user/email "paul@ex.com" identity identity)]
      (is (= 0 (count (:deletions cl))))
      (is (= 0 (count (:updates cl))))
      (let [p-entid (core/save-new-entity @conn
                                          (save-new-user-txnmap user-partition
                                                                {:user/name "Paul"
                                                                 :user/email "paul@ex.com"}))
            p-lm (core/txn-time @conn p-entid)
            t2 (.toDate (t/now))
            d-entid (core/save-new-entity @conn
                                          (save-new-user-txnmap user-partition
                                                                {:user/name "Dave"
                                                                 :user/email "dave@ex.com"}))
            cl (core/change-log-since @conn t1 :user/email "paul@ex.com" identity identity)
            cl-2 (core/change-log-since @conn t2 :user/email "paul@ex.com" identity identity)
            cl-3 (core/change-log-since @conn t2 :user/email "dave@ex.com" identity identity)
            t3 (.toDate (t/now))
            cl-4 (core/change-log-since @conn t3 :user/email "dave@ex.com" identity identity)]
        (is (= 0 (count (:deletions cl))))
        (is (= 1 (count (:updates cl))))
        (is (= 0 (count (:deletions cl-2))))
        (is (= 0 (count (:updates cl-2))))
        (is (= 0 (count (:deletions cl-3))))
        (is (= 1 (count (:updates cl-3))))
        (is (= 0 (count (:deletions cl-4))))
        (is (= 0 (count (:updates cl-4))))
        (let [cl (core/change-log-since @conn t1 :user/email "dave@ex.com" identity identity)
              cl-2 (core/change-log-since @conn t1 :order/user p-entid identity identity)]
          (is (= 0 (count (:deletions cl))))
          (is (= 1 (count (:updates cl))))
          (is (= 0 (count (:deletions cl-2))))
          (is (= 0 (count (:updates cl-2))))
          (let [p-order-1 (core/save-new-entity @conn
                                                (save-new-order-txnmap user-partition
                                                                       p-entid
                                                                       {:order/user p-entid
                                                                        :order/name "Paul's order 1"}))]
            (let [cl (core/change-log-since @conn t1 :order/user p-entid identity identity)]
              (is (= 0 (count (:deletions cl))))
              (is (= 1 (count (:updates cl))))
              @(d/transact @conn [{:db/id p-order-1
                                   :order/name "Paul's updated order 1"}])
              (let [cl (core/change-log-since @conn t1 :order/user p-entid identity identity)]
                (is (= 0 (count (:deletions cl))))
                (is (= 1 (count (:updates cl))))
                @(d/transact @conn [[:db.fn/retractEntity p-order-1]])
                (let [cl (core/change-log-since @conn t1 :order/user p-entid identity identity)]
                  (is (= 1 (count (:deletions cl))))
                  (is (= 0 (count (:updates cl)))))))))))))
