(ns fx.repo.pg
  (:require
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as jdbc.rs]
   [next.jdbc.prepare :as jdbc.prep]
   [next.jdbc.types :refer [as-other]]
   [next.jdbc.date-time :as jdbc.dt]
   [honey.sql :as sql]
   [integrant.core :as ig]
   [malli.core :as m]
   [charred.api :as charred]
   [medley.core :as mdl]
   [fx.utils.honey]
   [fx.utils.types :refer [pgobject? connection?]]
   [fx.utils.common :refer [tap->]]
   [fx.entity]
   [fx.migrate :as migrate]
   [fx.repo :refer [IRepo]]
   [clojure.string :as str])
  (:import
   [javax.sql DataSource]
   [java.sql PreparedStatement Time Array]
   [org.postgresql.util PGobject PGInterval]
   [fx.entity Entity]
   [clojure.lang IPersistentMap IPersistentVector]
   [java.time Duration]))


(jdbc.dt/read-as-local)


(defn ->column-name
  "Add quoting for table name if field spec include :wrap flag"
  [entity field-name]
  (if (fx.entity/entity-field-prop entity field-name :wrap)
    [:quote field-name]
    (-> field-name name (str/replace "-" "_") keyword)))

(def column-name?
  [:or :keyword
   [:tuple [:= :quote] :keyword]])

(m/=> ->column-name
  [:=> [:cat fx.entity/entity? :keyword]
   column-name?])


;; =============================================================================
;; Postgres Array helpers
;; =============================================================================

(extend-protocol jdbc.rs/ReadableColumn
  Array
  (read-column-by-label [^Array v _]
    (vec (.getArray v)))
  (read-column-by-index [^Array v _ _]
    (vec (.getArray v))))


;; =============================================================================
;; Postgres JSON helpers
;; =============================================================================

(defn ->pgobject
  "Transforms Clojure data to a PGobject that contains the data as JSON.
   PGObject type defaults to `jsonb` but can be changed via metadata key `:pgtype`"
  [x]
  (let [pgtype (or (:pgtype (meta x)) "jsonb")]
    (doto (PGobject.)
      (.setType pgtype)
      (.setValue (charred/write-json-str x)))))

(m/=> ->pgobject
  [:=> [:cat [:or map? vector?]]
   pgobject?])


(defn <-pgobject
  "Transform PGobject containing `json` or `jsonb` value to Clojure data."
  [^PGobject v]
  (let [type  (.getType v)
        value (.getValue v)]
    (if (#{"jsonb" "json"} type)
      (when value
        (with-meta (charred/read-json value :key-fn keyword) {:pgtype type}))
      value)))

(m/=> <-pgobject
  [:=> [:cat pgobject?]
   :any])


(extend-protocol jdbc.prep/SettableParameter
  IPersistentMap
  (set-parameter [m ^PreparedStatement s i]
    (.setObject s i (->pgobject m)))

  IPersistentVector
  (set-parameter [v ^PreparedStatement s i]
    (.setObject s i (->pgobject v))))


(extend-protocol jdbc.rs/ReadableColumn
  PGobject
  (read-column-by-label [^PGobject v _]
    (<-pgobject v))
  (read-column-by-index [^PGobject v _2 _3]
    (<-pgobject v)))


;; =============================================================================
;; Postgres Time helpers
;; =============================================================================

(extend-protocol jdbc.rs/ReadableColumn
  Time
  (read-column-by-label [^Time v _]
    (.toLocalTime v))
  (read-column-by-index [^Time v _2 _3]
    (.toLocalTime v)))


;; =============================================================================
;; Postgres Interval helpers
;; =============================================================================

(defn ->pg-interval
  "Takes a Duration instance and converts it into a PGInterval
   instance where the interval is created as a number of seconds."
  [^Duration duration]
  (PGInterval. 0 0
               (.toDaysPart duration)
               (.toHoursPart duration)
               (.toMinutesPart duration)
               (.toSecondsPart duration)))


(extend-protocol jdbc.prep/SettableParameter
  ;; Convert durations to PGIntervals before inserting into db
  Duration
  (set-parameter [^Duration v ^PreparedStatement s ^long i]
    (.setObject s i (->pg-interval v))))


(defn <-pg-interval
  "Takes a PGInterval instance and converts it into a Duration
   instance. Ignore sub-second units."
  [^PGInterval interval]
  (-> Duration/ZERO
      (.plusSeconds (.getSeconds interval))
      (.plusMinutes (.getMinutes interval))
      (.plusHours (.getHours interval))
      (.plusDays (.getDays interval))))

(extend-protocol jdbc.rs/ReadableColumn
  ;; Convert PGIntervals back to durations
  PGInterval
  (read-column-by-label [^PGInterval v _]
    (<-pg-interval v))
  (read-column-by-index [^PGInterval v _2 _3]
    (<-pg-interval v)))


;; =============================================================================
;; Postgres query construct helpers
;; =============================================================================

(defn prep-join-query
  "Returns a SQL query vector to join rows from the referenced table"
  [{:keys [entity ref table-name ref-key ref-name nested]}]
  (let [ref-props        (fx.entity/properties ref)
        ref-table        (fx.entity/ref-entity-prop ref :table)
        ref-table-key    (keyword ref-table)
        ref-entity       (fx.entity/field-type-prop ref :entity)
        ref-query-params (if (map? nested)
                           (get nested ref-key)
                           {})]
    (cond
      (fx.entity/required-ref? ref-props)
      (let [ref-pk           (-> (fx.entity/ident-field-schema ref-entity)
                                 key)
            entity-ref-field (keyword (format "%s.%s" table-name ref-name))
            entity-ref-match [:= ref-pk entity-ref-field]]
        (sql/format {:select (or (:fields ref-query-params) [:*])
                     :from   ref-table-key
                     :where  (if (some? (:where ref-query-params))
                               [:and entity-ref-match (:where ref-query-params)]
                               entity-ref-match)}
                    {:quoted true}))

      (= :one-to-many (:rel-type ref-props))
      (let [target-field-name (-> (fx.entity/ref-field-schema ref-entity entity)
                                  key
                                  name)
            target-column     (keyword (format "%s.%s" ref-table target-field-name))
            entity-pk         (-> (fx.entity/ident-field-schema entity)
                                  key
                                  name)
            entity-pk-column  (keyword (format "%s.%s" table-name entity-pk))
            entity-ref-match  [:= target-column entity-pk-column]]
        (sql/format (mdl/assoc-some
                     {:select (or (:fields ref-query-params) [:*])
                      :from   ref-table-key
                      :where  (if (some? (:where ref-query-params))
                                [:and entity-ref-match (:where ref-query-params)]
                                entity-ref-match)}
                     :limit (:limit ref-query-params)
                     :offset (:offset ref-query-params)
                     :order-by (:order-by ref-query-params))
                    {:quoted true}))

      (= :many-to-many (:rel-type ref-props))
      (let [query-fields       (if (some? (:fields ref-query-params))
                                 (mapv #(->> % name (format "tt.%s") keyword)
                                       (:fields ref-query-params))
                                 [:tt.*])
            join-entity        (:join ref-props)
            join-table         (-> (fx.entity/prop join-entity :table)
                                   keyword)
            join-entity-key    (-> (fx.entity/ref-field-schema join-entity entity)
                                   key name)
            join-ref-key       (-> (fx.entity/ref-field-schema join-entity ref-entity)
                                   key name)
            ref-pk             (-> (fx.entity/ident-field-schema ref-entity)
                                   key name)
            ref-pk-column      (keyword (format "tt.%s" ref-pk))
            join-ref-column    (keyword (format "jt.%s" join-ref-key))

            entity-pk          (-> (fx.entity/ident-field-schema entity)
                                   key
                                   name)
            entity-pk-column   (keyword (format "%s.%s" table-name entity-pk))
            join-entity-column (keyword (format "jt.%s" join-entity-key))
            entity-ref-match   [:= join-entity-column entity-pk-column]]
        (sql/format (mdl/assoc-some
                     {:select query-fields
                      :from   [[join-table :jt]]
                      :join   [[ref-table-key :tt] [:= join-ref-column ref-pk-column]]
                      :where  (if (some? (:where ref-query-params))
                                [:and entity-ref-match (:where ref-query-params)]
                                entity-ref-match)}
                     :limit (:limit ref-query-params)
                     :offset (:offset ref-query-params)
                     :order-by (:order-by ref-query-params))
                    {:quoted true})))))

(def field-params?
  [:map
   [:fields {:optional true} [:vector :keyword]]
   [:where {:optional true} vector?]
   [:limit {:optional true} :int]
   [:offset {:optional true} :int]
   [:order-by {:optional true} vector?]])

(def nested-params?
  [:or
   :boolean
   [:vector :keyword]
   [:map-of :keyword field-params?]])

(m/=> prep-join-query
  [:=> [:cat [:map
              [:entity fx.entity/entity?]
              [:table-name :string]
              [:ref fx.entity/schema?]
              [:ref-name :string]
              [:ref-key :keyword]
              [:nested [:maybe nested-params?]]]]
   [:vector :string]])


(defn wrap-coalesce
  "Wrap query with coalesce expression"
  [query]
  (sql/format-expr
   [:coalesce
    [[:raw query]]
    [:inline "[]"]]))

(m/=> wrap-coalesce
  [:=> [:cat [:vector :string]]
   [:vector :string]])


(defn prep-json-query
  "Takes the join query for nested tables and wraps it with SQL query to convert nested records into JSON"
  [{:keys [ref table-name ref-name join-query]}]
  (let [temp-join-key (keyword (format "%s_%s" table-name ref-name))
        ref-props     (fx.entity/properties ref)]
    (cond
      (fx.entity/required-ref? ref-props)
      (sql/format {:select [[[:row_to_json temp-join-key]]]
                   :from   [[[:nest [:raw join-query]]
                             temp-join-key]]}
                  {:quoted true})

      (fx.entity/optional-ref? ref-props)
      (-> (sql/format {:select [[[:array_to_json [:array_agg [:row_to_json temp-join-key]]]]]
                       :from   [[[:nest [:raw join-query]]
                                 temp-join-key]]}
                      {:quoted true})
          wrap-coalesce))))

(m/=> prep-json-query
  [:=> [:cat [:map
              [:ref fx.entity/schema?]
              [:ref-name :string]
              [:table-name :string]
              [:join-query [:vector :string]]]]
   [:vector :string]])


(defn get-refs-list
  "Returns the list of ref fields. *nest* param is used to control which fields are required"
  [entity nested]
  (cond
    (true? nested)  ;; return all nested records
    (->> (fx.entity/entity-fields entity)
         (filter (fn [[_ field]]
                   (and (fx.entity/ref? field)
                        (some? (fx.entity/ref-entity-prop field :table))))))

    (vector? nested) ;; return only listed nested records
    (let [nested-set (set nested)]
      (->> (fx.entity/entity-fields entity)
           (filter (fn [[field-key field]]
                     (and (contains? nested-set field-key)
                          (fx.entity/ref? field)
                          (some? (fx.entity/ref-entity-prop field :table)))))))

    (map? nested)   ;; fine-grained control of nested entities
    (let [nested-set (set (keys nested))]
      (->> (fx.entity/entity-fields entity)
           (filter (fn [[field-key field]]
                     (and (contains? nested-set field-key)
                          (fx.entity/ref? field)
                          (some? (fx.entity/ref-entity-prop field :table)))))))

    :else []))

(m/=> get-refs-list
  [:=> [:cat fx.entity/entity? [:maybe nested-params?]]
   [:sequential fx.entity/entity-field-schema?]])


(defn entity-select-query
  "Build the main entity SQL query as HoneySQL DSL"
  [entity fields nested]
  (let [table-name    (fx.entity/prop entity :table)
        table         (keyword table-name)
        default-query {:select (or fields [:*])
                       :from   [table]}
        refs          (get-refs-list entity nested)]
    (reduce (fn [query [ref-key ref]]
              (let [ref-name   (str/replace (name ref-key) "-" "_")
                    join-query (prep-join-query
                                {:entity     entity
                                 :ref        ref
                                 :table-name table-name
                                 :ref-key    ref-key
                                 :ref-name   ref-name
                                 :nested     nested})
                    json-query (prep-json-query
                                {:ref        ref
                                 :table-name table-name
                                 :ref-name   ref-name
                                 :join-query join-query})]
                (update query :select conj [[:nest [:raw json-query]] ref-key])))
            default-query
            refs)))

(m/=> entity-select-query
  [:=> [:cat fx.entity/entity? [:maybe [:vector :keyword]] [:maybe nested-params?]]
   [:map
    [:select vector?]
    [:from vector?]]])


(defn coerce-nested-records
  "Nested records returned as JSON. This function will use nested entity spec to coerce record fields"
  [entity record]
  (let [refs (->> (fx.entity/entity-fields entity)
                  (filter (fn [[_ field]] (fx.entity/ref? field))))]
    (reduce (fn [rec [ref-key ref]]
              (let [ref-entity (fx.entity/field-type-prop ref :entity)]
                (update rec ref-key
                        (fn [field-val]
                          (cond
                            (sequential? field-val)
                            (mapv #(fx.entity/cast ref-entity %) field-val)

                            (map? field-val)
                            (fx.entity/cast ref-entity field-val)

                            :else
                            field-val)))))
            record
            refs)))

(m/=> coerce-nested-records
  [:=> [:cat fx.entity/entity? map?]
   map?])


(defn simple-val-or-nested-entity?
  "Predicate function to check if field spec refers to a simple data type
   or json field or mandatory dependency entity"
  [field-schema]
  (or (not (fx.entity/ref? field-schema))
      (let [props     (fx.entity/properties field-schema)
            ref-table (fx.entity/ref-entity-prop field-schema :table)]
        (or (not ref-table)
            (not (fx.entity/optional-ref? props))))))

(m/=> simple-val-or-nested-entity?
  [:=> [:cat fx.entity/schema?]
   :boolean])


(defn entity-columns
  "Collect columns names as keywords"
  [entity]
  (->> (fx.entity/entity-fields entity)
       (filter #(simple-val-or-nested-entity? (val %)))
       (mapv key)))

(m/=> entity-columns
  [:=> [:cat fx.entity/entity?]
   [:vector :keyword]])


(defn lift-value
  "Wraps vector or map with :lift operator to store them as json"
  [x]
  (if (or (vector? x) (map? x))
    [:lift x]
    x))


(defn prep-value
  [field-schema v]
  (let [enum?  (and (fx.entity/ref? field-schema)
                    (some? (fx.entity/ref-entity-prop field-schema :enum)))
        array? (= (-> field-schema fx.entity/field-schema :type) :array)]
    (cond
      enum? (as-other v)
      array? (into-array v)
      :else (lift-value v))))


(defn prep-data
  "Builds a list of columns names and respective values based on the field schema"
  [entity data]
  (reduce-kv
   (fn [[columns values :as acc] k v]
     (if-some [field (val (fx.entity/entity-field entity k))]
       (if (simple-val-or-nested-entity? field)
         (let [value  (prep-value field v)
               column (->column-name entity k)]
           [(conj columns column)
            (conj values value)])
         acc)
       ;; some junk in the data, noop
       acc))
   []
   data))


(defn prep-data-map [entity data]
  (reduce-kv
   (fn [acc k v]
     (if-some [field (fx.entity/entity-field entity k)]
       (let [field-schema (val field)]
         (if (simple-val-or-nested-entity? field-schema)
           (let [value  (prep-value field-schema v)
                 column (->column-name entity k)]
             (assoc acc column value))
           acc))
       ;; some junk in the data, noop
       acc))
   {}
   data))


(defn where-clause [where-clauses eq-clauses]
  (cond
    (and (some? where-clauses) (some? eq-clauses)) [:and where-clauses eq-clauses]
    (some? eq-clauses) eq-clauses
    (some? where-clauses) where-clauses
    :else nil))


(defn pg-save!
  "Save record in database"
  [^DataSource database entity data]
  ;; TODO deal with nested records (not JSON fields). Extract identity field if it's map
  (let [table (fx.entity/prop entity :table)
        [columns values] (prep-data entity data)
        query (sql/format {:insert-into table
                           :columns     columns
                           :values      [values]})]
    (jdbc/execute-one! database query
      {:return-keys true
       :builder-fn  jdbc.rs/as-unqualified-kebab-maps})))

(m/=> pg-save!
  [:=> [:cat connection? fx.entity/entity? map?]
   map?])


(defn pg-save-all!
  "Save records in database"
  [^DataSource database entity data]
  (jdbc/with-transaction [tx database]
    (mapv (fn [record]
            (pg-save! tx entity record))
          data)))

(m/=> pg-save-all!
  [:=> [:cat connection? fx.entity/entity? vector?]
   vector?])


(defn pg-update!
  "Update record in database"
  [^DataSource database entity data {:fx.repo/keys [where] :as params}]
  (let [table        (fx.entity/prop entity :table)
        eq-clauses   (some-> (prep-data-map entity params)
                             (not-empty)
                             (sql/map=))
        where-clause (where-clause where eq-clauses)
        query        (sql/format {:update-raw [:quote table]
                                  :set        (prep-data-map entity data)
                                  :where      where-clause})]
    (jdbc/execute-one! database query
      {:return-keys true
       :builder-fn  jdbc.rs/as-unqualified-kebab-maps})))

(m/=> pg-update!
  [:=> [:cat
        connection?
        fx.entity/entity?
        map?
        [:map [:fx.repo/where {:optional true} vector?]]]
   map?])


(defn pg-delete!
  "Delete record from database"
  [^DataSource database entity {:fx.repo/keys [where] :as params}]
  (let [table        (fx.entity/prop entity :table)
        eq-clauses   (some-> (prep-data-map entity params)
                             (not-empty)
                             (sql/map=))
        where-clause (where-clause where eq-clauses)
        query        (sql/format {:delete-from (keyword table)
                                  :where       where-clause})]
    (jdbc/execute-one! database query
      {:return-keys true
       :builder-fn  jdbc.rs/as-unqualified-kebab-maps})))

(m/=> pg-delete!
  [:=> [:cat
        connection?
        fx.entity/entity?
        [:map [:fx.repo/where {:optional true} vector?]]]
   map?])


(defn pg-find!
  "Get single record from the database"
  [^DataSource database entity {:fx.repo/keys [fields where nested] :as params}] ;; TODO add exclude parameter to filter fields
  (let [eq-clauses (some-> (prep-data-map entity params)
                           (not-empty)
                           (sql/map=))
        where-map  {:where (where-clause where eq-clauses)}
        select-map (entity-select-query entity fields nested)
        query      (-> select-map
                       (merge where-map)
                       (sql/format {:quoted true})
                       (tap-> "Find query"))
        record     (jdbc/execute-one! database query
                     {:return-keys true
                      :builder-fn  jdbc.rs/as-unqualified-kebab-maps})]
    (println "nested" nested)
    (if (some? nested)
      (coerce-nested-records entity record)
      record)))

(m/=> pg-find!
  [:=> [:cat
        connection?
        fx.entity/entity?
        [:map
         [:fx.repo/fields {:optional true} [:vector :keyword]]
         [:fx.repo/where {:optional true} vector?]
         [:fx.repo/nested {:optional true} nested-params?]]]
   [:maybe map?]])


(defn pg-find-all!
  "Return multiple records from the database"
  [^DataSource database entity {:fx.repo/keys [fields where order-by limit offset nested] :as params}]
  (let [eq-clauses (some-> (prep-data-map entity params)
                           (not-empty)
                           (sql/map=))
        rest-map   (mdl/assoc-some
                    {}
                    :where (where-clause where eq-clauses)
                    :limit limit
                    :offset offset
                    :order-by order-by)
        select-map (entity-select-query entity fields nested)
        query      (-> select-map
                       (merge rest-map)
                       (sql/format {:quoted true}))
        records    (jdbc/execute! database query
                     {:return-keys true
                      :builder-fn  jdbc.rs/as-unqualified-kebab-maps})]
    (if (some? nested)
      (mapv #(coerce-nested-records entity %) records)
      records)))

(m/=> pg-find-all!
  [:=> [:cat
        connection?
        fx.entity/entity?
        [:map
         [:fx.repo/fields {:optional true} [:vector :keyword]]
         [:fx.repo/where {:optional true} vector?]
         [:fx.repo/nested {:optional true} nested-params?]
         [:fx.repo/order-by {:optional true} vector?]
         [:fx.repo/limit {:optional true} :int]
         [:fx.repo/offset {:optional true} :int]]]
   [:maybe [:vector map?]]])


;; =============================================================================
;; Duct integration
;; =============================================================================

(defmethod ig/init-key :fx.repo.pg/migrate [_ {:keys [strategy] :as config}]
  (let [{:keys [rollback-migrations]}
        (case strategy
          (:update :update-drop) (migrate/apply-migrations! config)
          :store (migrate/store-migrations! config)
          :validate (migrate/validate-schema! config)
          nil)]
    (assoc config :rollback-migrations rollback-migrations)))


(defmethod ig/halt-key! :fx.repo.pg/migrate [_ {:keys [^DataSource database strategy rollback-migrations]}]
  (when (= strategy :update-drop)
    (migrate/drop-migrations! database rollback-migrations)))


(defmethod ig/init-key :fx.repo.pg/adapter [_ {:keys [database]}]
  (extend-protocol IRepo
    Entity
    (save! [entity data]
      (pg-save! database entity data))

    (save-all! [entity data]
      (pg-save-all! database entity data))

    (update! [entity data params]
      (pg-update! database entity data params))

    (delete! [entity params]
      (pg-delete! database entity params))

    (find! [entity params]
      (pg-find! database entity params))

    (find-all!
      ([entity]
       (pg-find-all! database entity {}))
      ([entity params]
       (pg-find-all! database entity params)))))
