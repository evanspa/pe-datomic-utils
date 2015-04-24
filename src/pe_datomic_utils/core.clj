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

(defn datoms-of-attrs-since
  "Returns the datoms from the log where each datom's entity id matches entid
  and its attribute entity ID is of an attribute entity ID belonging to attrs,
  and with a transaction date as of since-inst."
  [conn entid attrs since-inst]
  (let [attr-entids (map #(entity-id-for-schema-attribute conn %) attrs)]
    (let [txns (->> (seq (d/tx-range (d/log conn) since-inst nil))
                    (reduce (fn [txns txn]
                              (let [{datums :data} txn]
                                (concat txns
                                        (filter #(and (some #{(.a %)} attr-entids)
                                                      (= (.e %) entid))
                                                datums))))
                            []))]
      [txns attr-entids])))

(defn entities-of-attrs-since
  "Returns the set of entity IDs (including retracted ones) that contains at
  least one attribute from attrs, and exists as of since-inst."
  [conn attrs since-inst]
  (let [db (d/db conn)
        attr-entids (map #(entity-id-for-schema-attribute conn %) attrs)]
    (let [txns (->> (seq (d/tx-range (d/log conn) since-inst nil))
                    (reduce (fn [txns txn]
                              (let [{datums :data} txn]
                                (concat txns
                                        (filter #(some #{(.a %)} attr-entids)
                                                datums))))
                            []))]
      (filter (fn [entid]
                (d/entid db entid))
              (distinct (map #(.e %) txns))))))

(defn entities-updated-since
  "Returns the set of entity IDs that contain the attribute reqd-attr, and exist
  since since-inst.  If transform-fn is supplied, it will be applied against
  each found entity."
  ([conn
    since-inst
    reqd-attr
    reqd-attr-val]
   (entities-updated-since conn since-inst reqd-attr reqd-attr-val nil))
  ([conn
    since-inst
    reqd-attr
    reqd-attr-val
    transform-fn]
   (let [db (d/db conn)
         log (d/log conn)]
     (let [qry '[:find ?e ?op
                 :in ?log ?t1 ?reqd-attr ?reqd-attr-val $
                 :where [(tx-ids ?log ?t1 nil) [?tx ...]]
                        [(tx-data ?log ?tx) [[?e _ _ _ ?op]]]
                        [$ ?e ?reqd-attr ?reqd-attr-val]]
           results (d/q qry log since-inst reqd-attr reqd-attr-val db)]
       (when (> (count results) 0)
         (->>
          (remove nil?
                  (map (fn [result-tuple]
                         (let [entid (first result-tuple)
                               entity (into {:db/id entid} (d/entity db entid))]
                           (if transform-fn
                             (transform-fn entity)
                             entity)))
                       results))
          (distinct)))))))

(defn is-entity-updated-since
  [conn since-inst entid]
  (let [db (d/db conn)
        since-entity (into {} (d/entity (d/since db since-inst) entid))]
    (and (not (empty? since-entity))
         (not= (into {} (d/entity (d/as-of db since-inst) entid))
               since-entity))))

(defn is-entity-deleted-since
  [conn since-inst entid]
  (let [db (d/db conn)]
    (and (not (empty? (into {} (d/entity (d/as-of db since-inst) entid))))
         (empty? (into {} (d/entity (d/since db since-inst) entid))))))

(defn are-attributes-retracted-since
  "Returns true if the entity with ID entid has attributes attrs all retracted
  as of since-inst.  Otherwise returns false."
  [conn entid attrs since-inst]
  (let [[datoms attr-entids] (datoms-of-attrs-since conn entid attrs since-inst)
        db (d/db conn)]
    (every? identity
            (map (fn [attr-entid]
                   (let [datoms (filter #(= (.a %) attr-entid) datoms)
                         datoms (sort (fn [d1 d2]
                                        (let [tx-inst-1 (:db/txInstant (d/entity db (.tx d1)))
                                              tx-inst-2 (:db/txInstant (d/entity db (.tx d2)))]
                                          (if (= tx-inst-1 tx-inst-2)
                                            (compare (.added d1) (.added d2))
                                            (compare tx-inst-1 tx-inst-2))))
                                      datoms)
                         last-datom (last datoms)]
                     (when last-datom
                       (not (.added last-datom)))))
                 attr-entids))))

(defn change-log-since
  "Returns a map with 2 keys: :updates and :deletions.  The value at each key is
  a vector of entities that have either been updated (add/update) or deleted as
  of since-inst.  Each vector contains a collection of entries as maps.
  filter-fn will be invoked for each candidate entity and will return a boolean
  indicating if it should be included in the final set. The parameters
  updated-entry-maker-fn and deleted-entry-maker-fn are used to construct the
  maps.  updated-entry-maker-fn will be used to contruct the maps to go into the
  :updates vector; deleted-entry-maker-fn will be used to construct the maps to
  go into the :deletions vector.  Each of these functions will receive the
  populated Datomic entity, and is to return a map.  Updates and deletions of
  entities containing reqd-attr will be included in the computation."
  [conn
   since-inst
   reqd-attr
   reqd-attr-val
   updated-entry-maker-fn
   deleted-entry-maker-fn]
  {:updates (entities-updated-since conn
                                    since-inst
                                    reqd-attr
                                    reqd-attr-val
                                    updated-entry-maker-fn)
   :deletions (let [ent-ids (entities-of-attrs-since conn [reqd-attr] since-inst)]
                (reduce (fn [deleted-entities ent-id]
                          (let [is-deleted (are-attributes-retracted-since conn
                                                                           ent-id
                                                                           [reqd-attr]
                                                                           since-inst)]
                            (if is-deleted
                              (conj deleted-entities
                                    (deleted-entry-maker-fn {:db/id ent-id}))
                              deleted-entities)))
                        []
                        ent-ids))})
