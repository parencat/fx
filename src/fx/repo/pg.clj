(ns fx.repo.pg
  (:require
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as jdbc.rs]
   [next.jdbc.prepare :as jdbc.prep]
   [honey.sql :as sql]
   [integrant.core :as ig]
   [malli.core :as m]
   [charred.api :as charred]
   [medley.core :as mdl]
   [fx.utils.honey]
   [fx.utils.types :refer [pgobject? connection?]]
   [fx.entity]
   [fx.migrate :as migrate]
   [fx.repo :refer [IRepo]])
  (:import
   [java.sql Connection PreparedStatement]
   [org.postgresql.util PGobject]
   [fx.entity Entity]
   [clojure.lang IPersistentMap IPersistentVector]))


(defn ->column-name
  "Add quoting for table name if field spec include :wrap? flag"
  [entity field-name]
  (if (fx.entity/field-prop entity field-name :wrap?)
    [:quote field-name]
    field-name))

(def column-name?
  [:or :keyword
   [:tuple [:= :quote] :keyword]])

(m/=> ->column-name
  [:=> [:cat fx.entity/entity? :keyword]
   column-name?])


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
;; Postgres query construct helpers
;; =============================================================================

(defn prep-join-query
  "Returns a SQL query vector to join rows from the referenced table"
  [{:keys [entity ref table-name ref-key ref-name nested]}]
  (let [ref-props        (fx.entity/properties ref)
        ref-table        (fx.entity/ref-field-prop ref :table)
        ref-table-key    (keyword ref-table)
        ref-entity       (fx.entity/ref-type-prop ref :entity)
        ref-query-params (if (map? nested)
                           (get nested ref-key)
                           {})]
    (cond
      (or (:one-to-one? ref-props)
          (:many-to-one? ref-props))
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

      (and (:one-to-many? ref-props)
           (not (:join-table ref-props)))
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

      (or (:many-to-many? ref-props)
          (and (:one-to-many? ref-props)
               (some? (:join-table ref-props))))
      (let [query-fields       (if (some? (:fields ref-query-params))
                                 (mapv #(->> % name (format "tt.%s") keyword)
                                       (:fields ref-query-params))
                                 [:tt.*])
            ref-pk             (-> (fx.entity/ident-field-schema ref-entity)
                                   key
                                   name)
            ref-pk-column      (keyword (format "tt.%s" ref-pk))
            join-table         (:join-table ref-props)
            join-ref-column    (keyword (format "jt.%s-%s" ref-table ref-pk))
            entity-pk          (-> (fx.entity/ident-field-schema entity)
                                   key
                                   name)
            entity-pk-column   (keyword (format "%s.%s" table-name entity-pk))
            join-entity-column (keyword (format "jt.%s-%s" table-name entity-pk))
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
      (or (:one-to-one? ref-props)
          (:many-to-one? ref-props))
      (sql/format {:select [[[:row_to_json temp-join-key]]]
                   :from   [[[:nest [:raw join-query]]
                             temp-join-key]]}
                  {:quoted true})

      (or (:one-to-many? ref-props)
          (:many-to-many? ref-props))
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
  "Returns the list of ref fields. *nest* param is used to control which fields is required"
  [entity nested]
  (cond
    (true? nested)  ;; return all nested records
    (->> (fx.entity/entity-all-fields entity)
         (filter (fn [[_ field]] (fx.entity/ref? field))))

    (vector? nested) ;; return only listed nested records
    (let [nested-set (set nested)]
      (->> (fx.entity/entity-all-fields entity)
           (filter (fn [[field-key field]]
                     (and (contains? nested-set field-key)
                          (fx.entity/ref? field))))))

    (map? nested)   ;; fine-grained control of nested entities
    (let [nested-set (set (keys nested))]
      (->> (fx.entity/entity-all-fields entity)
           (filter (fn [[field-key field]]
                     (and (contains? nested-set field-key)
                          (fx.entity/ref? field))))))

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
              (let [ref-name   (name ref-key)
                    join-query (prep-join-query {:entity     entity
                                                 :ref        ref
                                                 :table-name table-name
                                                 :ref-key    ref-key
                                                 :ref-name   ref-name
                                                 :nested     nested})
                    json-query (prep-json-query {:ref        ref
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
  (let [refs (->> (fx.entity/entity-all-fields entity)
                  (filter (fn [[_ field]] (fx.entity/ref? field))))]
    (reduce (fn [rec [ref-key ref]]
              (let [ref-entity (fx.entity/ref-type-prop ref :entity)]
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


(defn pg-save!
  "Save record in database"
  [^Connection database entity data]
  (let [table   (fx.entity/prop entity :table)
        columns (fx.entity/entity-columns entity)
        values  (fx.entity/entity-values entity data)
        query   (-> {:insert-into table
                     :columns     (mapv #(->column-name entity %) columns)
                     :values      [values]}
                    (sql/format))]
    (jdbc/execute-one! database query
      {:return-keys true
       :builder-fn  jdbc.rs/as-unqualified-kebab-maps})))

(m/=> pg-save!
  [:=> [:cat connection? fx.entity/entity? map?]
   map?])


(defn pg-update!
  "Update record in database"
  [^Connection database entity data {:keys [where] :as params}]
  (let [table        (fx.entity/prop entity :table)
        columns      (fx.entity/entity-columns entity)
        eq-clauses   (some-> (select-keys params columns)
                             (not-empty)
                             (sql/map=))
        where-clause (if (some? eq-clauses)
                       [:and where eq-clauses]
                       where)
        query        (-> {:update (keyword table)
                          :set    data
                          :where  where-clause}
                         (sql/format {:quoted true}))]
    (jdbc/execute-one! database query
      {:return-keys true
       :builder-fn  jdbc.rs/as-unqualified-kebab-maps})))

(m/=> pg-update!
  [:=> [:cat connection? fx.entity/entity? map? [:map [:where {:optional true} vector?]]]
   map?])


(defn pg-delete!
  "Delete record from database"
  [^Connection database entity {:keys [where] :as params}]
  (let [table        (fx.entity/prop entity :table)
        columns      (fx.entity/entity-columns entity)
        eq-clauses   (some-> (select-keys params columns)
                             (not-empty)
                             (sql/map=))
        where-clause (if (some? eq-clauses)
                       [:and where eq-clauses]
                       where)
        query        (-> {:delete-from (keyword table)
                          :where       where-clause}
                         (sql/format {:quoted true}))]
    (jdbc/execute-one! database query
      {:return-keys true
       :builder-fn  jdbc.rs/as-unqualified-kebab-maps})))

(m/=> pg-delete!
  [:=> [:cat connection? fx.entity/entity? [:map [:where {:optional true} vector?]]]
   map?])


(defn pg-find!
  "Get single record from the database"
  [^Connection database entity {:keys [fields where nested] :as params}]
  (let [columns    (fx.entity/entity-columns entity)
        eq-clauses (some-> (select-keys params columns)
                           (not-empty)
                           (sql/map=))
        where-map  {:where (if (some? eq-clauses)
                             [:and where eq-clauses]
                             where)}
        select-map (entity-select-query entity fields nested)
        query      (-> select-map
                       (merge where-map)
                       (sql/format {:quoted true}))
        record     (jdbc/execute-one! database query
                     {:return-keys true
                      :builder-fn  jdbc.rs/as-unqualified-kebab-maps})]
    (if (some? nested)
      (coerce-nested-records entity record)
      record)))

(m/=> pg-find!
  [:=> [:cat connection? fx.entity/entity? [:map
                                            [:fields {:optional true} [:vector :keyword]]
                                            [:where {:optional true} vector?]
                                            [:nested {:optional true} nested-params?]]]
   [:maybe map?]])


(defn pg-find-all!
  "Return multiple records from the database"
  [^Connection database entity {:keys [fields where order-by limit offset nested] :as params}]
  (let [columns    (fx.entity/entity-columns entity)
        eq-clauses (some-> (select-keys params columns)
                           (not-empty)
                           (sql/map=))
        rest-map   (mdl/assoc-some
                    {}
                    :where (cond
                             (and (some? where) (some? eq-clauses)) [:and where eq-clauses]
                             (some? where) where
                             :else nil)
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
  [:=> [:cat connection? fx.entity/entity? [:map
                                            [:fields {:optional true} [:vector :keyword]]
                                            [:where {:optional true} vector?]
                                            [:nested {:optional true} nested-params?]
                                            [:order-by {:optional true} vector?]
                                            [:limit {:optional true} :int]
                                            [:offset {:optional true} :int]]]
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


(defmethod ig/halt-key! :fx.repo.pg/migrate [_ {:keys [^Connection database strategy rollback-migrations]}]
  (when (= strategy :update-drop)
    (migrate/drop-migrations! database rollback-migrations)))


(defmethod ig/init-key :fx.repo.pg/adapter [_ {:keys [database]}]
  (extend-protocol IRepo
    Entity
    (save! [entity data]
      (pg-save! database entity data))

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
