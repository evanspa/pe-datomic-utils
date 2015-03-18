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

(defn entity-id-for-schema-attribute
  "Returns the entity ID of attr."
  [conn attr]
  (let [results (d/q '[:find ?attr-ent
                       :in $ ?attr
                       :where [$ ?attr-ent :db/ident ?attr]]
                     (d/db conn)
                     attr)]
    (when (> (count results) 0)
      (ffirst results))))

(defn datoms-of-attrs-as-of
  "Returns the datoms from the log where each datom's entity id matches entid
  and its attribute entity ID is of an attribute entity ID belonging to attrs,
  and with a transaction date as of as-of-inst."
  [conn entid attrs as-of-inst]
  (let [attr-entids (map #(entity-id-for-schema-attribute conn %) attrs)]
    (let [txns (->> (seq (d/tx-range (d/log conn) as-of-inst nil))
                    (reduce (fn [txns txn]
                              (let [{datums :data} txn]
                                (concat txns
                                        (filter #(and (some #{(.a %)} attr-entids)
                                                      (= (.e %) entid))
                                                datums))))
                            []))]
      [txns attr-entids])))

(defn entities-of-attrs-as-of
  "Returns the set of entity IDs (including retracted ones) that contains at
  least one attribute from attrs, and exists as of as-of-inst."
  [conn attrs as-of-inst]
  (let [attr-entids (map #(entity-id-for-schema-attribute conn %) attrs)]
    (let [txns (->> (seq (d/tx-range (d/log conn) as-of-inst nil))
                    (reduce (fn [txns txn]
                              (let [{datums :data} txn]
                                (concat txns
                                        (filter #(some #{(.a %)} attr-entids)
                                                datums))))
                            []))]
      (distinct (map #(.e %) txns)))))

(defn entities-updated-as-of
  ([conn
    as-of-instant
    attrs]
   (entities-updated-as-of conn as-of-instant attrs nil))
  ([conn
    as-of-instant
    attrs
    transform-fn]
   (let [db (d/db conn)
         log (d/log conn)]
     (reduce (fn [entities attr]
               (let [qry '[:find ?e ?op
                           :in ?log ?t1 ?attr $
                           :where [(tx-ids ?log ?t1 nil) [?tx ...]]
                           [(tx-data ?log ?tx) [[?e _ _ _ ?op]]]
                           [$ ?e ?attr _]]
                     results (d/q qry log as-of-instant attr db)]
                 (if (> (count results) 0)
                   (apply conj
                          entities
                          (map (fn [result-tuple]
                                 (let [user-entid (first result-tuple)
                                       entity (into {:db/id user-entid} (d/entity db user-entid))]
                                   (if transform-fn
                                     (transform-fn entity)
                                     entity)))
                               results))
                   entities)))
             []
             attrs))))

(defn are-attributes-retracted-as-of
  [conn entid attrs as-of-inst]
  (let [[datoms attr-entids] (datoms-of-attrs-as-of conn entid attrs as-of-inst)]
    (every? identity
            (map (fn [attr-entid]
                   (let [datoms (filter #(= (.a %) attr-entid) datoms)
                         last-datom (last datoms)]
                     (when last-datom
                       (not (.added last-datom)))))
                 attr-entids))))

(defn change-log
  "Returns a map with 2 keys: :updates and :deletions.  The value at each key is
  a vector of entities that have either been updated (add/update) or deleted as
  of as-of-instant.  Each vector contains a collection of entries as maps.  The
  parameters updated-entry-maker-fn and deleted-entry-maker-fn are used to
  construct the maps.  updated-entry-maker-fn will be used to contruct the maps
  to go into the :updates vector; deleted-entry-maker-fn will be used to
  construct the maps to go into the :deletions vector.  Each of these functions
  will receive the populated Datomic entity, and is to return a map.  Updates
  and deletions of entities containing reqd-attr will be included in the
  computation."
  [conn
   as-of-instant
   reqd-attr
   updated-entry-maker-fn
   deleted-entry-maker-fn]
  {:updates (entities-updated-as-of conn as-of-instant [reqd-attr] updated-entry-maker-fn)
   :deletions (let [ent-ids (entities-of-attrs-as-of conn [reqd-attr] as-of-instant)]
                (reduce (fn [deleted-entities ent-id]
                          (let [is-deleted (are-attributes-retracted-as-of conn
                                                                           ent-id
                                                                           [reqd-attr]
                                                                           as-of-instant)]
                            (if is-deleted
                              (conj deleted-entities
                                    (deleted-entry-maker-fn {:db/id ent-id}))
                              deleted-entities)))
                        []
                        ent-ids))})
