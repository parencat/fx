(ns fx.migrate
  (:require
   [clojure.string :as str]
   [clojure.set]
   [clojure.core.match :refer [match]]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [fx.entity :as fx.entity]
   [honey.sql :as sql]
   [fx.utils.honey]
   [weavejester.dependency :as dep]
   [medley.core :as mdl]
   [differ.core :as differ]
   [malli.core :as m]
   [malli.util :as mu]
   [fx.utils.types :refer [connection? clock?]]
   [clojure.java.io :as io])
  (:import
   [javax.sql DataSource]
   [java.sql DatabaseMetaData Connection]
   [java.time Clock]))


;; =============================================================================
;; Entity DDL
;; =============================================================================

(declare schema->column-type)


(def column-type
  [:or :keyword [:cat :keyword [:* [:or :int :string]]]])


(def table-field-constraints
  [:map
   [:optional {:optional true} :boolean]
   [:primary-key {:optional true} :boolean]
   [:foreign-key {:optional true} :string]])

(def table-field
  (mu/merge
   [:map
    [:type column-type]]
   table-field-constraints))

(def table-fields
  [:map-of :keyword table-field])


(defn get-ref-type
  "Given an entity name will find a primary key field and return its type.
   e.g. :my/user -> :uuid
   Type could be complex e.g. [:string 250]"
  [entity-key]
  (let [table (fx.entity/prop entity-key :table)
        enum  (fx.entity/prop entity-key :enum)]
    (cond
      (some? table)
      (-> entity-key
          fx.entity/ident-field-schema
          val
          fx.entity/field-schema
          schema->column-type)

      (some? enum)
      (schema->column-type {:type :enum :props enum})

      :else
      (schema->column-type {:type :jsonb :props nil}))))

(m/=> get-ref-type
  [:=> [:cat :qualified-keyword]
   column-type])


(defn ->array-type
  "Returns supported by Postgres array type"
  [type]
  (case type
    (:smallint) "int2"
    (:int :integer 'int? 'integer?) "int4"
    (:bigint) "int8"
    (:real 'float?) "float4"
    (:double 'double? :decimal :numeric 'number?) "float8"
    (:boolean 'boolean?) "bool"
    (:char 'char? :string 'string?) "varchar"
    (throw (ex-info "Not supported type for array" {:type type}))))

(m/=> ->array-type
  [:=> [:cat [:or :keyword :symbol]]
   :string])


(defn schema->column-type
  "Given a field type and optional properties will return a unified (simplified) type representation.
   Few implementation notes:
   interval with fields - only DAY, HOUR, MINUTE and SECOND supported due to conversion from Duration class
   time with timezone doesn't really work - zone part is missed somewhere in between DB and PGDriver"
  [{:keys [type props]}]
  (match [type props]
    ;; uuid
    [(:or :uuid 'uuid?) _] :uuid

    ;; numeric
    [:smallint _] :smallint
    [:bigint _] :bigint
    [(:or :int :integer 'int? 'integer?) _] :integer
    [(:or :decimal :numeric 'number?) {:precision p :scale s}] [:numeric p s]
    [(:or :decimal :numeric 'number?) {:precision p}] [:numeric p]
    [(:or :decimal :numeric 'number?) _] :numeric
    [(:or :real 'float?) _] :real
    [(:or :double 'double?) _] [:double-precision]
    [:smallserial _] :smallserial
    [:serial _] :serial
    [:bigserial _] :bigserial

    ;; character
    [(:or :char 'char?) {:max max}] [:char max]
    [(:or :string 'string?) {:max max}] [:varchar max]
    [(:or :string 'string?) _] :varchar

    [(:or :boolean 'boolean?) _] :boolean
    [:enum enum-name] (keyword enum-name)
    [:jsonb _] :jsonb
    [:array {:of atype}] [:array (->array-type atype)]

    [:timestamp _] :timestamp
    [:timestamp-tz _] [:timestamp-tz]
    [:date _] :date
    [:time _] :time
    [:time-tz _] [:time-tz]
    [:interval {:fields fields}] [:interval fields]
    [:interval _] :interval

    [:entity-ref {:entity entity-key}] (get-ref-type entity-key)

    :else (throw (ex-info (str "Unknown type for column " type) {:type type}))))

(m/=> schema->column-type
  [:=> [:cat [:map
              [:type [:or :keyword :symbol]]
              [:props [:maybe [:or :map :string :keyword]]]]]
   column-type])


(defn ->constraint-name [table column constraint]
  (let [valid-column-name (-> column name (str/replace "-" "_"))]
    (format "%s_%s_%s" table valid-column-name constraint)))


(defn schema->column-modifiers
  "Given an entity schema will return a vector of SQL field constraints, shaped as HoneySQL clauses
   e.g. [[:not nil] [:primary-key]]"
  [entity field-key field-schema]
  (let [{:keys [default optional identity reference unique cascade]}
        (fx.entity/properties field-schema)
        table     (fx.entity/prop entity :table)
        ref-table (delay (fx.entity/ref-entity-prop field-schema :table))]
    (cond-> []
            (not optional) (conj [:not nil])
            (some? default) (conj [:default default])
            unique (conj [:named-constraint (->constraint-name table field-key "unique") [:unique]])
            identity (conj [:primary-key])

            (and reference (some? @ref-table))
            (conj [:constraint [:quote (->constraint-name table field-key "fkey")]]
                  [:references [:quote @ref-table]])
            cascade (conj [:cascade]))))

(m/=> schema->column-modifiers
  [:=> [:cat fx.entity/entity? :keyword fx.entity/schema?]
   [:vector vector?]])


(defn ->column-name [entity field-name]
  (if (fx.entity/entity-field-prop entity field-name :wrap)
    [:quote field-name]
    field-name))

(def column-name?
  [:or :keyword
   [:tuple [:= :quote] :keyword]])

(m/=> ->column-name
  [:=> [:cat fx.entity/entity? :keyword]
   column-name?])


(defn schema->constraints-map
  "Converts entity field spec to a map of fields constraints
   used primarily for diffing states
   e.g. {:optional false :primary-key true}"
  [entry-schema]
  (let [{:keys [default optional identity reference unique cascade]}
        (fx.entity/properties entry-schema)
        ref-table (delay (fx.entity/ref-entity-prop entry-schema :table))]
    (cond-> {}
            (some? optional) (assoc :optional optional)
            unique (assoc :unique true)
            identity (assoc :primary-key true)
            (some? default) (assoc :default default)
            (and reference (some? @ref-table)) (assoc :foreign-key @ref-table)
            cascade (assoc :cascade true))))

(m/=> schema->constraints-map
  [:=> [:cat fx.entity/schema?]
   map?])


(defn get-entity-columns
  "Given an entity will return a simplified representation of its fields as Clojure map
   e.g. {:id    {:type uuid?   :optional false :primary-key true}
          :name {:type string? :optional true}}"
  [entity]
  (->> (fx.entity/entity-fields entity)
       (filter (fn [[_ field-schema]]
                 (or (not (fx.entity/ref? field-schema))
                     (let [props     (fx.entity/properties field-schema)
                           ref-table (fx.entity/ref-entity-prop field-schema :table)]
                       (or (not ref-table)
                           (not (fx.entity/optional-ref? props)))))))
       (reduce (fn [acc [key schema]]
                 (let [column (-> {:type (-> schema fx.entity/field-schema schema->column-type)}
                                  (merge (schema->constraints-map schema)))]
                   (assoc acc key column)))
               {})))

(m/=> get-entity-columns
  [:=> [:cat fx.entity/entity?]
   table-fields])


;; =============================================================================
;; DB DDL
;; =============================================================================

(defn table-exist?
  "Checks if table exists in database"
  [^DataSource database table]
  (let [^Connection conn           (jdbc/get-connection database)
        ^DatabaseMetaData metadata (.getMetaData conn)
        ;; TODO pass a schema option as well as table
        tables                     (-> metadata
                                       (.getTables nil nil table nil))]
    (.next tables)))

(m/=> table-exist?
  [:=> [:cat connection? :string]
   :boolean])


(def index-by-name
  (partial mdl/index-by #(or (:column-name %)
                             (:fkcolumn-name %))))


(defn extract-db-columns
  "Returns all table fields metadata"
  [^DataSource database table]
  (let [^Connection conn           (jdbc/get-connection database)
        ^DatabaseMetaData metadata (.getMetaData conn)
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
                                       index-by-name)
        unique-keys                (-> metadata
                                       (.getIndexInfo nil nil table true false)
                                       (rs/datafiable-result-set database {:builder-fn rs/as-unqualified-kebab-maps})
                                       index-by-name)]
    (mdl/deep-merge columns primary-keys foreign-keys unique-keys)))

(def raw-table-field
  [:map
   [:type-name :string]
   [:column-size :int]
   [:nullable [:enum 0 1]]
   [:column-def [:maybe :string]]
   [:non-unique {:optional true} :boolean]
   [:pk-name {:optional true} :string]
   [:pktable-name {:optional true} :string]
   [:fkcolumn-name {:optional true} :string]
   [:delete-rule {:optional true} :int]])

(m/=> extract-db-columns
  [:=> [:cat connection? :string]
   [:map-of :string raw-table-field]])


(defn extract-default-val
  "Parse column definition and return a default value"
  [column-def]
  (let [default (re-find #"[^::]+" column-def)]
    (if (str/starts-with? default "'")
      (subs default 1 (- (count default) 1))
      default)))


(defn column->constraints-map
  "Convert table field map to field constraints map"
  [{:keys [nullable pk-name fkcolumn-name pktable-name column-def non-unique delete-rule]}]
  (cond-> {}
          (= nullable 1) (assoc :optional true)
          (and (some? pk-name)
               (not fkcolumn-name)) (assoc :primary-key true)
          (some? fkcolumn-name) (assoc :foreign-key pktable-name)
          (string? column-def) (assoc :default (extract-default-val column-def))
          (and (some? non-unique)
               (not (some? pk-name))) (assoc :unique (not non-unique))
          (= delete-rule DatabaseMetaData/importedKeyCascade) (assoc :cascade true)))

(m/=> column->constraints-map
  [:=> [:cat raw-table-field]
   table-field-constraints])


(def default-column-size
  2147483647)       ;; TODO Not very reliable number


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

(m/=> get-db-columns
  [:=> [:cat connection? :string]
   table-fields])


;; =============================================================================
;; Migration functions
;; =============================================================================

(defn alter-table-ddl
  "Returns HoneySQL formatted map representing alter SQL clause"
  [table changes]
  (when-not (empty? changes)
    (sql/format {:alter-table
                 (into [table] changes)})))


(defn column->modifiers
  "Converts field to HoneySQL vector definition"
  [entity col-name column]
  (let [{:keys [optional default unique primary-key foreign-key cascade]} column
        table (fx.entity/prop entity :table)]
    (cond-> []
            (not optional) (conj [:not nil])
            (some? default) (conj [:default default])
            (true? unique) (conj [:named-constraint (->constraint-name table col-name "unique") [:unique]])
            primary-key (conj [:primary-key])
            (some? foreign-key) (conj [:constraint [:quote (->constraint-name table col-name "fkey")]]
                                      [:references [:quote foreign-key]])
            cascade (conj [:cascade]))))

(m/=> column->modifiers
  [:=> [:cat fx.entity/entity? :keyword table-field-constraints]
   vector?])


(defn get-ref-table [entity field]
  (-> (fx.entity/entity-field entity field)
      (val)
      (fx.entity/ref-entity-prop :table)))


(defn ->set-ddl
  "Converts table fields to the list of HoneySQL alter clauses"
  [entity columns]
  (let [table (fx.entity/prop entity :table)]
    (->
     (for [[column column-spec] columns]
       (let [column-name (->column-name entity column)
             column-fk   (->constraint-name table column "fkey")]
         (for [[op value] column-spec]
           (match [op value]
             [:type _] {:alter-column [column-name :type value]}
             [:optional true] {:alter-column [column-name :set [:not nil]]}
             [:optional false] {:alter-column [column-name :drop [:not nil]]}
             [:primary-key true] {:add-index [:primary-key column-name]}
             [:primary-key false] {:drop-index [:primary-key column-name]}
             [:foreign-key ref] {:add-constraint [[:quote column-fk]
                                                  [:foreign-key column-name]
                                                  [:references [:quote ref]]]}
             [:foreign-key false] {:drop-constraint [[:quote column-fk]]}
             [:cascade true] [{:drop-constraint [[:quote column-fk]]}
                              {:add-constraint [[:quote column-fk]
                                                [:foreign-key column-name]
                                                [:references [:quote (get-ref-table entity column)]]
                                                [:cascade]]}]
             [:cascade false] [{:drop-constraint [[:quote column-fk]]}
                               {:add-constraint [[:quote column-fk]
                                                 [:foreign-key column-name]
                                                 [:references [:quote (get-ref-table entity column)]]
                                                 [:no-action]]}]
             [:default default] {:alter-column-raw [column-name :set [:default default]]}
             [:unique true] {:add-constraint [[:raw (->constraint-name table column "unique")] [:unique nil column-name]]}
             [:unique false] {:drop-constraint [[:raw (->constraint-name table column "unique")]]}))))
     flatten)))

(m/=> ->set-ddl
  [:=> [:cat fx.entity/entity? [:map-of :keyword map?]]
   [:sequential map?]])


(defn ->constraints-drop-ddl
  "Converts table fields to the list of HoneySQL drop clauses"
  [entity columns]
  (let [table (fx.entity/prop entity :table)]
    (->
     (for [[column column-spec] columns]
       (let [column-name (->column-name entity column)
             column-fk   (->constraint-name table column "fkey")]
         (for [[op value] column-spec]
           (match [op value]
             [:optional 0] {:alter-column [column-name :drop [:not nil]]}
             [:primary-key 0] {:drop-index [:primary-key column-name]}
             [:foreign-key 0] {:drop-constraint [[:quote column-fk]]}
             [:cascade 0] [{:drop-constraint [[:quote column-fk]]}
                           {:add-constraint [[:quote column-fk]
                                             [:foreign-key column-name]
                                             [:references [:quote (get-ref-table entity column)]]]}]
             [:default 0] {:alter-column [column-name :drop [:default]]}
             [:unique 0] {:drop-constraint [[:raw (->constraint-name table column "unique")]]}))))
     flatten)))


(m/=> ->constraints-drop-ddl
  [:=> [:cat fx.entity/entity? [:map-of :keyword map?]]
   [:sequential map?]])


(defn ->add-ddl [entity all-columns cols-to-add]
  (mapv (fn [col-name]
          (let [column (get all-columns col-name)]
            (->> (column->modifiers entity col-name column)
                 (into [(->column-name entity col-name) (:type column)])
                 (hash-map :add-column-raw))))
        cols-to-add))

(m/=> ->add-ddl
  [:=> [:cat fx.entity/entity? table-fields [:set :keyword]]
   [:vector
    [:map [:add-column-raw vector?]]]])


(defn ->drop-ddl [entity cols-to-delete]
  (mapv #(hash-map :drop-column (->column-name entity %))
        cols-to-delete))

(m/=> ->drop-ddl
  [:=> [:cat fx.entity/entity? [:set :keyword]]
   [:vector
    [:map [:drop-column column-name?]]]])


(defn prep-changes
  "Given the simplified existing and updated fields definition
   will return a set of HoneySQL clauses to eliminate the difference"
  [entity db-columns entity-columns]
  (let [entity-fields  (-> entity-columns keys set)
        db-fields      (-> db-columns keys set)
        cols-to-add    (clojure.set/difference entity-fields db-fields)
        cols-to-delete (clojure.set/difference db-fields entity-fields)
        common-cols    (clojure.set/intersection db-fields entity-fields)
        [alterations deletions] (differ/diff (select-keys db-columns common-cols)
                                             (select-keys entity-columns common-cols))
        [rb-alterations rb-deletions] (differ/diff (select-keys entity-columns common-cols)
                                                   (select-keys db-columns common-cols))]
    {:updates   (concat (->drop-ddl entity cols-to-delete)
                        (->add-ddl entity entity-columns cols-to-add)
                        (->set-ddl entity alterations)
                        (->constraints-drop-ddl entity deletions))
     :rollbacks (concat (->drop-ddl entity cols-to-add)
                        (->constraints-drop-ddl entity rb-deletions)
                        (->add-ddl entity db-columns cols-to-delete)
                        (->set-ddl entity rb-alterations))}))

(m/=> prep-changes
  [:=> [:cat fx.entity/entity? table-fields table-fields]
   [:map
    [:updates [:sequential map?]]
    [:rollbacks [:sequential map?]]]])


(defn update-table
  "Adds two SQL commands to update fields and to roll back all updates"
  [database entity table migrations]
  (let [db-columns     (get-db-columns database table)
        entity-columns (get-entity-columns entity)
        {:keys [updates rollbacks]} (prep-changes entity db-columns entity-columns)]
    (cond-> migrations
            (not-empty updates)
            (conj (alter-table-ddl table updates)
                  (alter-table-ddl table rollbacks)))))

(m/=> update-table
  [:=> [:cat connection? fx.entity/entity? :string vector?]
   vector?])


(defn entity->columns-ddl
  "Converts entity spec to a list of HoneySQL vectors representing individual fields
   e.g. [[:id :uuid [:not nil] [:primary-key]] ...]"
  [entity]
  (let [pk      (fx.entity/prop entity :identity)
        columns (->> (fx.entity/entity-fields entity)
                     (filter (fn [[_ field-schema]]
                               (or (not (fx.entity/ref? field-schema))
                                   (let [props     (fx.entity/properties field-schema)
                                         ref-table (fx.entity/ref-entity-prop field-schema :table)]
                                     (or (not ref-table)
                                         (not (fx.entity/optional-ref? props)))))))
                     (mapv (fn [[field-key schema]]
                             (let [column-name (->column-name entity field-key)]
                               (-> [column-name [:inline (-> schema fx.entity/field-schema schema->column-type)]]
                                   (concat (schema->column-modifiers entity field-key schema))
                                   vec)))))]
    (cond-> columns
            (some? pk)
            (conj (->> pk
                       (into [:primary-key])
                       vector)))))

(m/=> entity->columns-ddl
  [:=> [:cat fx.entity/entity?]
   [:vector vector?]])


(defn create-table-ddl
  "Returns HoneySQL formatted map representing create SQL clause"
  [table columns]
  (sql/format {:create-table     table
               :with-columns-raw columns}))


(defn drop-table-ddl
  "Returns HoneySQL formatted map representing drop SQL clause"
  [table]
  (sql/format {:drop-table (keyword table)} {:quoted true}))


(defn create-table
  "Adds two SQL commands to create DB table and to delete this table"
  [entity table migrations]
  (let [ddl    (entity->columns-ddl entity)
        create (create-table-ddl table ddl)
        drop   (drop-table-ddl table)]
    (conj migrations create drop)))

(m/=> create-table
  [:=> [:cat fx.entity/entity? :string vector?]
   vector?])


(defn create-enum-ddl
  "Returns HoneySQL formatted map representing create enum SQL clause"
  [enum values]
  (sql/format {:create-enum enum
               :with-values values}))


(defn drop-enum-ddl
  "Returns HoneySQL formatted map representing drop enum SQL clause"
  [enum]
  (sql/format {:drop-enum enum}))


(defn create-enum [entity enum migrations]
  (let [enum-values (fx.entity/prop entity :values)]
    ;; TODO add alter type option
    (conj migrations (create-enum-ddl enum enum-values) (drop-enum-ddl enum))))

(m/=> create-enum
  [:=> [:cat fx.entity/entity? :string vector?]
   vector?])


(defn entity->migration
  "Given an entity will check if some updates were introduced
   If so will return a set of SQL migrations string"
  [^DataSource database migrations entity]
  (let [table (fx.entity/prop entity :table)
        enum  (fx.entity/prop entity :enum)]
    (cond
      (and (some? table)
           (not (table-exist? database table)))
      (create-table entity table migrations)

      (some? table)
      (update-table database entity table migrations)

      (some? enum)
      (create-enum entity enum migrations)

      :else
      migrations)))

(m/=> entity->migration
  [:=> [:cat connection? [:vector [:vector :string]] fx.entity/entity?]
   [:vector [:vector :string]]])


(defn sort-by-dependencies
  "According to dependencies between entities will sort them in the topological order"
  [entities]
  (let [graph (atom (dep/graph))
        emap  (into {} (map (fn [e] [(:type e) e])) entities)]
    ;; build the graph
    (doseq [e1 entities e2 entities]
      (if (fx.entity/depends-on? e1 e2)
        (reset! graph (dep/depend @graph (:type e1) (:type e2)))
        (reset! graph (dep/depend @graph (:type e1) nil))))
    ;; graph -> sorted list
    (->> @graph
         (dep/topo-sort)
         (remove nil?)
         (map (fn [e] (get emap e))))))

(m/=> sort-by-dependencies
  [:=> [:cat [:sequential fx.entity/entity?]]
   [:sequential fx.entity/entity?]])


(defn unzip
  "Reverse operation to interleave function
   e.g. [1 2 3 4] -> ([1 3] [2 4])"
  [coll]
  (->> coll
       (partition 2 2 (repeat nil))
       (apply map vector)))

(m/=> unzip
  [:=> [:cat sequential?]
   [:sequential {:min 2 :max 2} vector?]])


(def migratable-props
  #{:table :enum})


(defn has-migration? [entity]
  (->> (fx.entity/entity-properties entity)
       (some #(contains? migratable-props (key %)))
       (boolean)))

(m/=> has-migration?
  [:=> [:cat fx.entity/entity?]
   :boolean])


(defn clean-up-entities [entities]
  (->> entities
       (filter has-migration?)
       sort-by-dependencies))

(m/=> clean-up-entities
  [:=> [:cat [:set fx.entity/entity?]]
   [:sequential fx.entity/entity?]])


(defn get-all-migrations
  "Returns a two-dimensional vector of migration strings for all changed entities.
   For each entity will be two items 'SQL to apply changes' followed with 'SQL to drop changes'"
  [^DataSource database entities]
  (let [cln-entities (clean-up-entities entities)]
    (reduce (fn [migrations entity]
              (entity->migration database migrations entity))
            [] cln-entities)))

(m/=> get-all-migrations
  [:=> [:cat connection? [:set fx.entity/entity?]]
   [:vector [:vector :string]]])


(defn get-entity-migrations-map
  "Returns a map of shape {:entity/name {:up 'SQL to apply changes' :down 'SQL to drop changes'}}"
  [^DataSource database entities]
  (let [cln-entities (clean-up-entities entities)]
    (reduce (fn [migrations-map entity]
              (let [migration (entity->migration database [] entity)]
                (if (seq migration)
                  (assoc migrations-map (:type entity) {:up   (first migration)
                                                        :down (second migration)})
                  migrations-map)))
            {} cln-entities)))

(m/=> get-entity-migrations-map
  [:=> [:cat connection? [:set fx.entity/entity?]]
   [:map-of :qualified-keyword [:map
                                [:up [:vector :string]]
                                [:down [:vector :string]]]]])


(defn prep-migrations
  "Generates migrations for all entities in the system (forward and backward)"
  [^DataSource database entities]
  (let [all-migrations (get-all-migrations database entities)
        [migrations rollback-migrations] (unzip all-migrations)]
    {:migrations          migrations
     :rollback-migrations (-> rollback-migrations reverse vec)}))

(m/=> prep-migrations
  [:=> [:cat connection? [:set fx.entity/entity?]]
   [:map
    [:migrations [:vector [:vector :string]]]
    [:rollback-migrations [:vector [:vector :string]]]]])


(defn has-changes?
  "Given the simplified existing and updated fields definition
   will return true if there's a difference between them, otherwise false"
  [db-columns entity-columns]
  (let [entity-fields (-> entity-columns keys set)
        db-fields     (-> db-columns keys set)
        common-cols   (clojure.set/intersection db-fields entity-fields)
        [alterations deletions] (differ/diff (select-keys db-columns common-cols)
                                             (select-keys entity-columns common-cols))]
    (not (and (empty? alterations)
              (empty? deletions)))))

(m/=> has-changes?
  [:=> [:cat table-fields table-fields]
   :boolean])



(def vars-matcher
  "Regex that matches string template variables"
  #"\$\{[^\$\{\}]+\}")


(defn interpolate
  "Takes a template string with ${} placeholders and a hashmap with replacement values.
   Returns interpolated string"
  [template replacement]
  (str/replace template
               vars-matcher
               (fn [variable]
                 (let [end      (- (count variable) 1)
                       key-name (keyword (subs variable 2 end))]
                   (str (get replacement key-name ""))))))

(m/=> interpolate
  [:=> [:cat :string map?]
   :string])


(def default-path-pattern
  "resources/migrations/${timestamp}-${entity-ns}-${entity}.edn")


;; =============================================================================
;; Strategies
;; =============================================================================

(defn apply-migrations!
  "Generates and applies migrations related to entities on database
   All migrations run in a single transaction"
  [{:keys [^DataSource database entities]}]
  (try
    (let [{:keys [migrations rollback-migrations]} (prep-migrations database entities)]
      (jdbc/with-transaction [tx database]
        (doseq [migration migrations]
          (println "Running migration" migration)
          (jdbc/execute! tx migration)))
      {:rollback-migrations rollback-migrations})
    (catch Throwable t
      (println t)
      (throw t))))

(m/=> apply-migrations!
  [:=> [:cat [:map
              [:database connection?]
              [:entities [:set fx.entity/entity?]]]]
   [:map
    [:rollback-migrations [:vector [:vector :string]]]]])


(defn drop-migrations!
  "Rollback all changes made by apply-migrations! call"
  [^DataSource database rollback-migrations]
  (jdbc/with-transaction [tx database]
    (doseq [migration rollback-migrations]
      (println "Rolling back" migration)
      (jdbc/execute! tx migration))))

(m/=> drop-migrations!
  [:=> [:cat connection? [:vector [:vector :string]]]
   :nil])


(defn store-migrations!
  "Writes entities migrations code into files in .edn format"
  [{:keys [^DataSource database entities ^Clock clock path-pattern path-params]
    :or   {clock (Clock/systemUTC)}}]
  (let [migrations (get-entity-migrations-map database entities)
        timestamp  (.millis clock)]
    (doseq [[entity migration] migrations]
      (let [filename (interpolate (or path-pattern default-path-pattern)
                                  (merge path-params
                                         {:timestamp timestamp
                                          :entity-ns (namespace entity)
                                          :entity    (name entity)}))]
        (io/make-parents filename)
        (spit filename (str migration))))))

(m/=> store-migrations!
  [:=> [:cat [:map
              [:database connection?]
              [:entities [:set fx.entity/entity?]]
              [:clock {:optional true} clock?]
              [:path-pattern {:optional true} :string]
              [:path-params {:optional true} [:map-of :keyword :any]]]]
   :nil])


(defn validate-schema!
  "Compares DB schema with entities specs.
   Returns true if there's no changes false otherwise"
  [{:keys [^DataSource database entities]}]
  (->> entities
       (some (fn [entity]
               (let [table (fx.entity/prop entity :table)]
                 (and (table-exist? database table)
                      (let [db-columns     (get-db-columns database table)
                            entity-columns (get-entity-columns entity)]
                        (not (has-changes? db-columns entity-columns)))))))
       boolean))

(m/=> validate-schema!
  [:=> [:cat [:map
              [:database connection?]
              [:entities [:set fx.entity/entity?]]]]
   :boolean])
