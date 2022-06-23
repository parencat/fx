(ns fx.migrate
  (:require
   [integrant.core :as ig]
   [clojure.string :as str]
   [clojure.data :as data]
   [clojure.core.match :refer [match]]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [fx.entity :as fx.entity]
   [honey.sql :as sql]
   [weavejester.dependency :as dep])
  (:import
   [java.sql DatabaseMetaData]))


(sql/register-clause! :alter-column :modify-column :modify-column)


;; =============================================================================
;; Entity DDL
;; =============================================================================

(declare schema->column-type)


(defn get-ref-type [entity]
  (-> entity
      fx.entity/primary-key-schema
      val
      fx.entity/entry-schema
      schema->column-type))


(defn schema->column-type [{:keys [type props]}]
  (match [type props]
    [:uuid _] :uuid
    ['uuid? _] :uuid

    [:string {:max max}] [:varchar max]
    [:string _] :varchar
    ['string? _] :varchar

    [:entity-ref {:entity entity}] (get-ref-type entity)

    :else (throw (ex-info (str "Unknown type for column " type) {:type type}))))


(defn schema->column-modifiers [entry-schema]
  (let [props (fx.entity/properties entry-schema)]
    (cond-> []
            (not (:optional props)) (conj [:not nil])
            (:primary-key? props) (conj [:primary-key])
            (:foreign-key? props) (conj [:references [:raw (fx.entity/entry-schema-table entry-schema)]])
            (:cascade? props) (conj [:raw "on delete cascade"]))))


(defn entity->columns-ddl [entity]
  (->> (fx.entity/entity-entries (:entity entity))
       (mapv (fn [[key schema]]
               (-> [key (-> schema fx.entity/entry-schema schema->column-type)]
                   (concat (schema->column-modifiers schema))
                   vec)))))


;; =============================================================================
;; DB DDL
;; =============================================================================

(defn table-exist? [database table]
  (let [^DatabaseMetaData metadata (.getMetaData database)
        tables                     (-> metadata
                                       (.getTables nil nil table nil))]
    (.next tables)))


(defn extract-db-columns [database table]
  (let [^DatabaseMetaData metadata (.getMetaData database)
        columns                    (-> metadata
                                       (.getColumns nil "public" table nil) ;; TODO expose schema as option to clients
                                       (rs/datafiable-result-set database {:builder-fn rs/as-unqualified-kebab-maps}))
        primary-keys               (-> metadata
                                       (.getPrimaryKeys nil nil table)
                                       (rs/datafiable-result-set database {:builder-fn rs/as-unqualified-kebab-maps}))
        foreign-keys               (-> metadata
                                       (.getImportedKeys nil nil table)
                                       (rs/datafiable-result-set database {:builder-fn rs/as-unqualified-kebab-maps}))
        pk-set                     (->> primary-keys
                                        (map :column-name)
                                        set)
        fk-map                     (->> foreign-keys
                                        (map (fn [{:keys [fkcolumn-name pktable-name]}]
                                               [fkcolumn-name {:foreign-key true
                                                               :references  pktable-name}]))
                                        (into {}))]
    (reduce (fn [acc {:keys [column-name] :as column}]
              (let [column' (cond-> column
                                    (contains? pk-set column-name) (assoc :primary-key true)
                                    (contains? fk-map column-name) (merge (get fk-map column-name)))]
                (conj acc column')))
            []
            columns)))


(defn db-columns->columns-ddl [columns]
  (reduce (fn [acc {:keys [column-name type-name nullable primary-key foreign-key references]}]
            (let [column-key (-> column-name
                                 (str/replace "_" "-") ;; TODO expose as option to clients
                                 keyword)
                  type-key   (keyword type-name)
                  modifiers  (cond-> []
                                     (= nullable 0) (conj [:not nil])
                                     primary-key (conj [:primary-key])
                                     foreign-key (conj [:references [:raw references]]))
                  column     (concat [column-key type-key] modifiers)]
              (conj acc column)))
          []
          columns))


;; =============================================================================
;; Migration functions
;; =============================================================================

(defn create-table-ddl [table columns]
  {:create-table table
   :with-columns columns})


(defn alter-table-ddl [table {:keys [modify delete add]}]
  (let [changes (concat (map #(hash-map :drop-column %) delete)
                        (map #(hash-map :add-column %) add)
                        (map #(hash-map :alter-column %) modify))]
    {:alter-table
     (into [table] changes)}))


(defn ->named-maps [ddl]
  (into {} (map #(vector (first %) %)) ddl))


; when type has been changed then inject "TYPE" raw from
; ALTER COLUMN id VARCHAR NOT NULL PRIMARY KEY      to
; ALTER COLUMN id TYPE VARCHAR NOT NULL PRIMARY KEY
(defn inject-type [[name & rst]]
  (concat [name] [[:raw "TYPE"]] rst))


(defn merge-columns [col1 col2]
  (let [max-count (max (count col1) (count col2))
        result (loop [i      0
                      result []]
                 (if (= i max-count)
                   result
                   (recur
                     (inc i)
                     (conj result (or (get col1 i)
                                    (get col2 i))))))]
    (if (second col2)
      (inject-type result)
      result)))


(defn prep-changes [db-ddl entity-ddl]
  (let [db-ddl-cols     (->named-maps db-ddl)
        entity-ddl-cols (->named-maps entity-ddl)
        [db-changes entity-changes common] (data/diff db-ddl-cols entity-ddl-cols)
        all-keys        (set (concat (keys db-changes) (keys entity-changes)))]
    (when (not-empty all-keys)
      (reduce (fn [acc key]
                (cond
                  (and (or (contains? entity-changes key)
                           (contains? db-changes key))
                       (contains? common key))
                  (let [entity (get entity-changes key)
                        common (get common key)
                        column (if (some? entity)
                                 (merge-columns entity common)
                                 common)]
                    (update acc :modify conj column))

                  (and (contains? db-changes key)
                       (not (contains? common key)))
                  (update acc :delete conj key)

                  (and (contains? entity-changes key)
                       (not (contains? common key)))
                  (->> (get entity-changes key)
                       rest
                       (remove nil?)
                       (into [key])
                       (update acc :add conj))))
              {} all-keys))))


(defn entity->migration [database migrations entity]
  (let [table      (:table entity)
        entity-ddl (entity->columns-ddl entity)]
    (if (not (table-exist? database table))
      (->> entity-ddl
           (create-table-ddl table)
           (sql/format)
           (conj migrations))

      (let [db-columns (extract-db-columns database table)
            db-ddl     (db-columns->columns-ddl db-columns)
            changes    (prep-changes db-ddl entity-ddl)]
        (some->> changes
                 (alter-table-ddl table)
                 (sql/format)
                 (conj migrations))))))


(defn sort-by-dependencies [entities]
  (let [graph (atom (dep/graph))
        emap  (into {} (map (fn [e] [(:entity e) e])) entities)]
    (doseq [e1 entities e2 entities]
      (if (fx.entity/depends-on? (:entity e1) (:entity e2))
        (reset! graph (dep/depend @graph (:entity e1) (:entity e2)))
        (reset! graph (dep/depend @graph (:entity e1) nil))))

    (->> @graph
         (dep/topo-sort)
         (remove nil?)
         (map (fn [e] (get emap e))))))


(defn prep-migrations [database entities]
  (let [sorted-entities (sort-by-dependencies entities)]
    (-> (partial entity->migration database)
        (reduce [] sorted-entities))))


(defn apply-migrations [{:keys [database entities]}]
  (let [migrations (prep-migrations database entities)]
    (doseq [migration migrations]
      (println "Running migration" migration)
      (jdbc/execute! database migration))))


;; =============================================================================
;; Duct integration
;; =============================================================================

(defmethod ig/prep-key :fx/migrate [_ config]
  (merge config
         {:database (ig/ref :fx.database/connection)
          :entities (ig/refset :fx/entity)}))


(defmethod ig/init-key :fx/migrate [_ config]
  (apply-migrations config))
