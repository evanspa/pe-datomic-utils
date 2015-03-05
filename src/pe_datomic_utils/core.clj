(ns pe-datomic-utils.core
  "A set of helper functions for when working with Datomic."
  (:require [datomic.api :refer [q db] :as d]
            [clj-time.core :as t]
            [clojure.java.io :refer [resource]]
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
  "Transacts txnmap using conn and returns the generated entity id."
  [conn txnmap]
  (let [tx @(d/transact conn [txnmap])]
    (d/resolve-tempid (d/db conn) (:tempids tx) (:db/id txnmap))))

(defn transact-schema-files
  "Transacts the set of schema files using conn.  Each schema file is assumed to
  be on the classpath, and contains a single transaction."
  [conn schema-files]
  (doseq [schema-file schema-files]
    @(d/transact conn (read-string (slurp (resource schema-file))))))

(defn refresh-db
  "First deletes the database at db-uri, then creates it, and then transacts the
  set of schema files."
  [db-uri schema-files]
  (do
    (d/delete-database db-uri)
    (d/create-database db-uri)
    (let [conn (d/connect db-uri)]
      (transact-schema-files conn schema-files)
      conn)))

(defn transact-user-schema-attribute
  "Transacts a schema attribute with a :db/ident of attr-ident.  This attribute
  is of type long and cardinality one.  It's purpose is to capture the notion of
  a schema version; similar to SQLite's 'user_version' pragma."
  [conn attr-ident]
  @(d/transact conn [{:db/id #db/id[:db.part/db]
                      :db/ident attr-ident
                      :db/valueType :db.type/long
                      :db/cardinality :db.cardinality/one
                      :db/doc "The user schema version number."
                      :db.install/_attribute :db.part/db}]))

(defn transact-partition
  "Transacts the given partition using conn."
  [conn partition]
  @(d/transact conn [{:db/id (d/tempid :db.part/db)
                      :db/ident partition
                      :db.install/_partition :db.part/db}]))

(defn get-user-schema-version
  "Returns the schema version of the database associated with conn (assumes the
  schema attribute was installed using transact-user-schema-attribute)."
  [conn user-schema-attr-name]
  (let [result (d/q (format "[:find ?ver ?verval :in $ :where [?ver %s ?verval]]"
                            user-schema-attr-name)
                    (d/db conn))]

    (when (> (count result) 0)
      (first result))))

(defn transact-user-schema-version
  "Transacts a new schema version value val to the database associated with
  conn, in partition, where the schema attribute :db/ident is
  user-schema-attr-name."
  [conn partition user-schema-attr-name val]
  (let [existing-ver (get-user-schema-version conn user-schema-attr-name)
        ver-entid (if existing-ver (first existing-ver) (d/tempid partition))]
    @(d/transact conn [{:db/id ver-entid user-schema-attr-name val}])))

(defn entity-for-parent-by-id
  "Returns a vector where the 1st element is an entity id, and the 2nd element
  the corresponding entity map.  The entity information returned is a direct
  child entity of the entity with entity id entity-entid."
  [conn parent-entid entity-entid parent-attr]
  {:pre [(not (nil? parent-entid))
         (not (nil? entity-entid))]}
  (let [entity (into {} (d/entity (d/db conn) entity-entid))]
    ; if entity has no entries (count = 0), then effectively the given
    ; entity-entid is not associated with any entities in the database
    (when (and (> (count entity) 0)
               (= parent-entid (get-in entity [parent-attr :db/id])))
      [entity-entid (dissoc entity parent-attr)])))
