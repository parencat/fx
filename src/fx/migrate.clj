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
   [differ.core :as differ]
   [malli.core :as m]
   [malli.util :as mu])
  (:import
   [java.sql DatabaseMetaData Connection]))


;; =============================================================================
;; Entity DDL
;; =============================================================================

(declare schema->column-type)


(defn get-ref-type
  "Given an entity name will find a primary key field and return its type.
   e.g. :my/user -> :uuid
   Type could be complex e.g. [:string 250]"
  [entity]
  (-> entity
      fx.entity/primary-key-schema
      val
      fx.entity/entry-schema
      schema->column-type))

(m/=> get-ref-type
  [:=> [:cat :qualified-keyword]
   [:or :keyword [:tuple :keyword :int]]])


(defn schema->column-type
  "Given a field type and optional properties will return a unified (simplified) type representation"
  [{:keys [type props]}]
  (match [type props]
    [:uuid _] :uuid
    ['uuid? _] :uuid

    [:string {:max max}] [:varchar max]
    [:string _] :varchar
    ['string? _] :varchar

    [:entity-ref {:entity entity}] (get-ref-type entity)

    :else (throw (ex-info (str "Unknown type for column " type) {:type type}))))

(m/=> schema->column-type
  [:=> [:cat [:map
              [:type [:or :keyword :symbol]]
              [:props [:maybe :map]]]]
   [:or :keyword [:tuple :keyword :int]]])


(defn schema->column-modifiers
  "Given an entity schema will return a vector of SQL field constraints, shaped as HoneySQL clauses
   e.g. [[:not nil] [:primary-key]]"
  [entry-schema]
  (let [props (fx.entity/properties entry-schema)]
    (cond-> []
            (not (:optional props)) (conj [:not nil])
            (:primary-key? props) (conj [:primary-key])
            (:foreign-key? props) (conj [:references [:raw (fx.entity/entry-schema-table entry-schema)]])
            (:cascade? props) (conj [:raw "on delete cascade"]))))

(m/=> schema->column-modifiers
  [:=> [:cat fx.entity/schema?]
   [:vector vector?]])


(defn entity->columns-ddl
  "Converts entity spec to a list of HoneySQL vectors representing individual fields
   e.g. [[:id :uuid [:not nil] [:primary-key]] ...]"
  [entity]
  (->> (fx.entity/entity-entries (:entity entity))
       (mapv (fn [[key schema]]
               (-> [key (-> schema fx.entity/entry-schema schema->column-type)]
                   (concat (schema->column-modifiers schema))
                   vec)))))

(m/=> entity->columns-ddl
  [:=> [:cat fx.entity/entity?]
   [:vector vector?]])


(defn schema->constraints-map
  "Converts entity field spec to a map of fields constraints
   e.g. {:optional false :primary-key? true}"
  [entry-schema]
  (let [props (fx.entity/properties entry-schema)]
    (cond-> {}
            (some? (:optional props)) (assoc :optional (:optional props))
            (:primary-key? props) (assoc :primary-key? true)
            (:foreign-key? props) (assoc :foreign-key? (fx.entity/entry-schema-table entry-schema))
            (:cascade? props) (assoc :cascade? true))))

(m/=> schema->constraints-map
  [:=> [:cat fx.entity/schema?]
   [:map]])


(defn get-entity-columns
  "Given an entity will return a simplified representation of its fields as Clojure map
   e.g. {:id    {:type uuid?   :optional false :primary-key? true}
          :name {:type string? :optional true}}"
  [entity]
  (->> (fx.entity/entity-entries (:entity entity))
       (reduce (fn [acc [key schema]]
                 (let [column (-> {:type (-> schema fx.entity/entry-schema schema->column-type)}
                                  (merge (schema->constraints-map schema)))]
                   (assoc acc key column)))
               {})))

(m/=> get-entity-columns
  [:=> [:cat fx.entity/entity?]
   [:map-of :keyword :map]])


;; =============================================================================
;; DB DDL
;; =============================================================================

(defn table-exist?
  "Checks if table exists in database"
  [^Connection database table]
  (let [^DatabaseMetaData metadata (.getMetaData database)
        ;; TODO pass a schema option as well as table
        tables                     (-> metadata
                                       (.getTables nil nil table nil))]
    (.next tables)))

(def connection?
  (m/-simple-schema
   {:type :db/connection
    :pred #(instance? Connection %)}))

(m/=> table-exist?
  [:=> [:cat connection? :string]
   :boolean])


(def index-by-name
  (partial mdl/index-by :column-name))


(defn extract-db-columns
  "Returns all table fields metadata"
  [^Connection database table]
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

(def raw-table-field
  [:map
   [:type-name :string]
   [:column-size :int]
   [:nullable [:enum 0 1]]
   [:pk-name {:optional true} :string]
   [:fkcolumn-name {:optional true} :string]
   [:pktable-name {:optional true} :string]])

(m/=> extract-db-columns
  [:=> [:cat connection? :string]
   [:map-of :string raw-table-field]])


(defn column->constraints-map
  "Convert table field map to field constraints map"
  [{:keys [nullable pk-name fkcolumn-name pktable-name]}]
  (cond-> {}
          (= nullable 1) (assoc :optional true)
          (some? pk-name) (assoc :primary-key? true)
          (some? fkcolumn-name) (assoc :foreign-key? pktable-name)))
;;(:cascade? props) (assoc :cascade? true)))

(def table-field-constraints
  [:map
   [:optional {:optional true} :boolean]
   [:primary-key? {:optional true} :boolean]
   [:foreign-key? {:optional true} :string]])

(m/=> column->constraints-map
  [:=> [:cat raw-table-field]
   table-field-constraints])


(def default-column-size
  2147483647)


(defn get-db-columns
  "Fetches the table fields definition and convert them to simplified Clojure maps"
  [database table]
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

(def table-field
  (mu/merge
   [:map
    [:type [:or :keyword [:tuple :keyword :int]]]]
   table-field-constraints))

(def table-fields
  [:map-of :keyword table-field])

(m/=> get-db-columns
  [:=> [:cat connection? :string]
   table-fields])


;; =============================================================================
;; Migration functions
;; =============================================================================

;; Postgres doesn't support :modify-column clause
(sql/register-clause! :alter-column :modify-column :modify-column)
(sql/register-clause! :add-constraint :modify-column :modify-column)
(sql/register-clause! :drop-constraint :modify-column :modify-column)


(defn create-table-ddl
  "Returns HoneySQL formatted map representing create SQL clause"
  [table columns]
  {:create-table table
   :with-columns columns})


(defn alter-table-ddl
  "Returns HoneySQL formatted map representing alter SQL clause"
  [table changes]
  (when-not (empty? changes)
    {:alter-table
     (into [table] changes)}))


(defn column->modifiers
  "Converts field to HonetSQL vector definition"
  [column]
  (cond-> []
          (not (:optional column)) (conj [:not nil])
          (:primary-key? column) (conj [:primary-key])
          (some? (:foreign-key? column)) (conj [:references [:raw (:foreign-key? column)]])
          (:cascade? column) (conj [:raw "on delete cascade"])))

(m/=> column->modifiers
  [:=> [:cat table-field-constraints]
   vector?])


(defn ->set-ddl
  "Converts table fields to the list of HoneySQL alter clauses"
  [columns]
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


(defn ->drop-ddl
  "Converts table fields to the list of HoneySQL drop clauses"
  [columns]
  (->
   (for [[column-name column] columns]
     (for [[op value] column]
       (match [op value]
         [:optional 0] {:alter-column [column-name :drop [:not nil]]}
         [:primary-key? 0] {:drop-index [:primary-key column-name]}
         [:foreign-key? 0] {:drop-constraint [(str (name column-name) "-fk")]})))
   flatten))


(defn prep-changes
  "Given the simplified existing and updated fields definition
   will return a set of HoneySQL clauses to eliminate the difference"
  [db-columns entity-columns]
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


(defn entity->migration
  "Given an entity will check if some updates were introduced
   If so will return a set of SQL migrations string"
  [^Connection database migrations entity]
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

(m/=> entity->migration
  [:=> [:cat connection? [:vector [:vector :string]] fx.entity/entity?]
   [:vector [:vector :string]]])


(defn sort-by-dependencies
  "According to dependencies between entities will sort them in the topological order"
  [entities]
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

(m/=> sort-by-dependencies
  [:=> [:cat [:sequential fx.entity/entity?]]
   [:sequential fx.entity/entity?]])


(defn prep-migrations
  "Generates migrations for all entities in the system"
  [^Connection database entities]
  (let [sorted-entities (sort-by-dependencies entities)]
    (-> (partial entity->migration database)
        (reduce [] sorted-entities))))

(m/=> prep-migrations
  [:=> [:cat connection? [:vector fx.entity/entity?]]
   [:vector [:vector :string]]])


(defn apply-migrations!
  "Generates and applies migrations related to entities on database
   All migrations run in a single transaction"
  [{:keys [^Connection database entities]}]
  (let [migrations (prep-migrations database entities)]
    (jdbc/with-transaction [tx database]
      (doseq [migration migrations]
        (println "Running migration" migration)
        (jdbc/execute! tx migration)))))

(m/=> apply-migrations!
  [:=> [:cat [:map
              [:database connection?]
              [:entities [:vector fx.entity/entity?]]]]
   :nil])


;; =============================================================================
;; Duct integration
;; =============================================================================

(defmethod ig/prep-key :fx/migrate [_ config]
  (merge config
         {:database (ig/ref :fx.database/connection)
          :entities (ig/refset :fx/entity)}))


(defmethod ig/init-key :fx/migrate [_ config]
  (apply-migrations! config))
