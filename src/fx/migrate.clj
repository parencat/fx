(ns fx.migrate
  (:require
   [integrant.core :as ig]
   [clojure.string :as str]
   [clojure.set]
   [clojure.core.match :refer [match]]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [fx.entity :as fx.entity]
   [honey.sql :as sql]
   [weavejester.dependency :as dep]
   [medley.core :as mdl]
   [differ.core :as differ])
  (:import
   [java.sql DatabaseMetaData]))


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


(defn schema->constraints-map [entry-schema]
  (let [props (fx.entity/properties entry-schema)]
    (cond-> {}
            (some? (:optional props)) (assoc :optional (:optional props))
            (:primary-key? props) (assoc :primary-key? true)
            (:foreign-key? props) (assoc :foreign-key? (fx.entity/entry-schema-table entry-schema))
            (:cascade? props) (assoc :cascade? true))))


(defn get-entity-columns [entity]
  (->> (fx.entity/entity-entries (:entity entity))
       (reduce (fn [acc [key schema]]
                 (let [column (-> {:type (-> schema fx.entity/entry-schema schema->column-type)}
                                  (merge (schema->constraints-map schema)))]
                   (assoc acc key column)))
               {})))


;; =============================================================================
;; DB DDL
;; =============================================================================

(defn table-exist? [database table]
  (let [^DatabaseMetaData metadata (.getMetaData database)
        tables                     (-> metadata
                                       (.getTables nil nil table nil))]
    (.next tables)))


(def index-by-name
  (partial mdl/index-by :column-name))


(defn extract-db-columns [database table]
  (let [^DatabaseMetaData metadata (.getMetaData database)
        columns                    (-> metadata
                                       (.getColumns nil "public" table nil) ;; TODO expose schema as option to clients
                                       (rs/datafiable-result-set database {:builder-fn rs/as-unqualified-kebab-maps})
                                       index-by-name)
        primary-keys               (-> metadata
                                       (.getPrimaryKeys nil nil table)
                                       (rs/datafiable-result-set database {:builder-fn rs/as-unqualified-kebab-maps})
                                       index-by-name)
        foreign-keys               (-> metadata
                                       (.getImportedKeys nil nil table)
                                       (rs/datafiable-result-set database {:builder-fn rs/as-unqualified-kebab-maps})
                                       index-by-name)]
    (mdl/deep-merge columns primary-keys foreign-keys)))


(defn column->constraints-map [{:keys [nullable pk-name fkcolumn-name pktable-name]}]
  (cond-> {}
          (= nullable 1) (assoc :optional true)
          (some? pk-name) (assoc :primary-key? true)
          (some? fkcolumn-name) (assoc :foreign-key? pktable-name)))
;;(:cascade? props) (assoc :cascade? true)))


(def default-column-size
  2147483647)


(defn get-db-columns [database table]
  (let [columns (extract-db-columns database table)]
    (mdl/map-kv
     (fn [column-name {:keys [type-name column-size] :as col}]
       (let [key      (-> column-name
                          (str/replace "_" "-") ;; TODO expose as option to clients
                          keyword)
             type-key (keyword type-name)
             type     (if (and (= type-key :varchar) (not= column-size default-column-size))
                        [type-key column-size]
                        type-key)
             column   (-> {:type type}
                          (merge (column->constraints-map col)))]
         [key column]))
     columns)))


;; =============================================================================
;; Migration functions
;; =============================================================================

;; Postgres doesn't support :modify-column clause
(sql/register-clause! :alter-column :modify-column :modify-column)
(sql/register-clause! :add-constraint :modify-column :modify-column)
(sql/register-clause! :drop-constraint :modify-column :modify-column)


(defn create-table-ddl [table columns]
  {:create-table table
   :with-columns columns})


(defn alter-table-ddl [table changes]
  (when-not (empty? changes)
    {:alter-table
     (into [table] changes)}))


(defn column->modifiers [column]
  (cond-> []
          (not (:optional column)) (conj [:not nil])
          (:primary-key? column) (conj [:primary-key])
          (some? (:foreign-key? column)) (conj [:references [:raw (:foreign-key? column)]])
          (:cascade? column) (conj [:raw "on delete cascade"])))


(defn ->set-ddl [columns]
  (->
    (for [[column-name column] columns]
      (for [[op value] column]
        (match [op value]
          [:type _] {:alter-column [column-name :type value]}
          [:optional true] {:alter-column [column-name :set [:not nil]]}
          [:optional false] {:alter-column [column-name :drop [:not nil]]}
          [:primary-key? true] {:add-index [:primary-key column-name]}
          [:primary-key? false] {:drop-index [:primary-key column-name]}
          [:foreign-key? ref] {:add-constraint [(str (name column-name) "-fk") [:foreign-key] [:references ref]]}
          [:foreign-key? false] {:drop-constraint [(str (name column-name) "-fk")]})))
    flatten))


(defn ->drop-ddl [columns]
  (->
    (for [[column-name column] columns]
      (for [[op value] column]
        (match [op value]
          [:optional 0] {:alter-column [column-name :drop [:not nil]]}
          [:primary-key? 0] {:drop-index [:primary-key column-name]}
          [:foreign-key? 0] {:drop-constraint [(str (name column-name) "-fk")]})))
    flatten))


(defn prep-changes [db-columns entity-columns]
  (let [entity-fields  (-> entity-columns keys set)
        db-fields      (-> db-columns keys set)
        cols-to-add    (clojure.set/difference entity-fields db-fields)
        cols-to-delete (clojure.set/difference db-fields entity-fields)
        common-cols    (clojure.set/intersection db-fields entity-fields)
        [alterations deletions] (differ/diff (select-keys db-columns common-cols)
                                             (select-keys entity-columns common-cols))]
    (concat (mapv #(hash-map :drop-column %)
                  cols-to-delete)
            (mapv (fn [col-name]
                    (let [column (get entity-columns col-name)]
                      (->> (column->modifiers column)
                           (into [col-name (:type column)])
                           (hash-map :add-column))))
                  cols-to-add)
            (->set-ddl alterations)
            (->drop-ddl deletions))))


(defn entity->migration [database migrations entity]
  (let [table (:table entity)]
    (if (not (table-exist? database table))
      (->> (entity->columns-ddl entity)
           (create-table-ddl table)
           (sql/format)
           (conj migrations))

      (let [db-columns     (get-db-columns database table)
            entity-columns (get-entity-columns entity)
            changes        (prep-changes db-columns entity-columns)]
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
    (jdbc/with-transaction [tx database]
      (doseq [migration migrations]
        (println "Running migration" migration)
        (jdbc/execute! tx migration)))))


;; =============================================================================
;; Duct integration
;; =============================================================================

(defmethod ig/prep-key :fx/migrate [_ config]
  (merge config
         {:database (ig/ref :fx.database/connection)
          :entities (ig/refset :fx/entity)}))


(defmethod ig/init-key :fx/migrate [_ config]
  (apply-migrations config))
