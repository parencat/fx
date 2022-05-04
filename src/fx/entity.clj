(ns fx.entity
  (:require
   [clojure.set :as c.set]
   [integrant.core :as ig]
   [fx.repository :as repo]
   [malli.core :as m]
   [malli.error :as me]
   [malli.registry :as mr]
   [next.jdbc :as jdbc]
   [honey.sql :as sql]))


(def rel-types
  #{:one-to-one? :one-to-many? :many-to-one? :many-to-many?})


(def EntityTableSpec
  [:schema
   {:registry
    {::table [:catn
              [:entity keyword?]
              [:properties [:? [:map-of keyword? any?]]]
              [:fields [:* [:schema [:ref ::field]]]]]
     ::field [:catn
              [:name keyword?]
              [:properties [:? [:map-of keyword? any?]]]
              [:type ::type]]
     ::type  [:altn
              [:fn-schema fn?]
              [:schema simple-keyword?]
              [:table-ref qualified-keyword?]
              [:type-with-props [:tuple keyword? map?]]]}}
   ::table])


(def parse-table
  (m/parser EntityTableSpec))


(defn name->ref [entity]
  (let [entity-ns       (namespace entity)
        entity-name     (name entity)
        entity-ref-name (str entity-name "-ref")]
    (keyword entity-ns entity-ref-name)))


(defmulti ->field-type
  (fn [_properties [type-key _type]] type-key))


(defmethod ->field-type :type-with-props
  [_properties [_type-key type]]
  (m/ast type))


(defmethod ->field-type :table-ref
  [{:keys [one-to-one? many-to-one?]} [_type-key type]]
  (let [entity-ref (name->ref type)]
    (if (or one-to-one? many-to-one?)
      {:type entity-ref}
      {:type     :+
       :children [{:type entity-ref}]})))


(defmethod ->field-type :default
  [_properties [_type-key type]]
  {:type type})


(defn ->field-properties [{:keys [one-two-many? many-two-many?] :as properties}]
  (cond-> properties
          :always
          (c.set/rename-keys {:optional? :optional})

          (or one-two-many? many-two-many?)
          (assoc :optional true)))


(defn field->spec-key [idx {:keys [name properties type]}]
  [name {:order      idx
         :value      (->field-type properties type)
         :properties (->field-properties properties)}])


(def entities-lookup
  (atom {}))


(def entities-registry
  (mr/lazy-registry
   (m/default-schemas)
   (fn [entity registry]
     (let [lookup @entities-lookup
           schema (some-> entity
                          lookup
                          (m/from-ast {:registry registry}))]
       schema))))


(defn register-lookup! [entity-name schema]
  (swap! entities-lookup assoc entity-name schema))


(defn register-entity! [entity-name fields]
  (let [schema-keys (->> fields
                         (map-indexed field->spec-key)
                         (into {}))
        pk-field    (->> fields
                         (filter #(get-in % [:properties :primary-key?]))
                         first)
        {:keys [name] [_ type] :type} pk-field

        schema      {:type :map :keys schema-keys}
        schema-ref  (m/schema [:or type [:map [name type]]])]

    (register-lookup! entity-name schema)
    (register-lookup! (name->ref entity-name) schema-ref)))


(defn get-validator-fn [entity-name]
  (fn [m]
    (or (m/validate entity-name m {:registry entities-registry})
        (throw (ex-info (str "Invalid data for entity " entity-name)
                        {:error (me/humanize (m/explain entity-name m {:registry entities-registry}))})))))


(defn get-pk-name [[field props]]
  (when (get-in props [:properties :primary-key?])
    field))


(defn add-related-entity [acc m name type-ref]
  (let [ref-fields (get-in @entities-lookup [type-ref :keys])
        value      (get m name)
        val        (if (map? value)
                     (get value (some get-pk-name ref-fields))
                     value)]
    (conj acc val)))


(defn get-values-fn [fields]
  (fn [m]
    (reduce (fn [acc {[type type-ref] :type :keys [name properties]}]
              (cond-> acc
                      (and (= type :table-ref)
                           (or (:one-to-one? properties) (:many-to-one? properties)))
                      (add-related-entity m name type-ref)

                      (not= type :table-ref)
                      (conj (get m name))))
            []
            fields)))


(defn get-entity-columns [fields]
  (->> fields
       (filter (fn [{[type] :type :keys [properties]}]
                 (not (and (= type :table-ref)
                           (or (:one-to-many? properties) (:many-to-many? properties))))))
       (mapv :name)))


(defrecord SQLEntity [database table]
  repo/PRepository
  (create! [_ entity-map]
    (let [{:keys [table-name columns validate get-values]} table]
      (validate entity-map)
      (let [query (-> {:insert-into table-name}
                      (assoc :columns columns)
                      (assoc :values [(get-values entity-map)])
                      (sql/format))]
        (jdbc/execute-one! database query))))

  (update! [_])
  (find! [_])
  (find-all! [_])
  (delete! [_]))


(defmethod ig/prep-key :fx/entity [_ table]
  {:table    table
   :database (ig/ref :fx/database)})


(defmethod ig/init-key :fx/entity [entity config]
  (let [{:keys [table database entity-type]
         :or   {entity-type :sql}} config
        entity-name  (if (vector? entity) (second entity) entity)
        valid-table? (m/validate EntityTableSpec table)]

    (when-not valid-table?
      (throw (ex-info (str "Invalid table schema for entity " entity-name)
                      {:error (me/humanize (m/explain EntityTableSpec table))})))

    (let [parsed-table (parse-table table)
          table-name   (get-in parsed-table [:properties :name])
          fields       (:fields parsed-table)
          columns      (get-entity-columns fields)
          get-values   (get-values-fn fields)
          validate     (get-validator-fn entity-name)
          entity-table {:table-name table-name
                        :fields     fields
                        :columns    columns
                        :validate   validate
                        :get-values get-values}]

      (register-entity! entity-name fields)

      (case entity-type
        :sql
        (map->SQLEntity {:database database
                         :table    entity-table})))))





(comment

 (require '[fx.database])

 (def user-tbl
   [:table {:name "user"}
    [:id {:primary-key? true} uuid?]
    [:name [:string {:max 250}]]
    [:last-name {:optional? true} string?]
    [:client {:many-to-one? true} :my/client]
    [:role {:many-to-one? true} :my/role]])

 (def client-tbl
   [:table {:name "client"}
    [:id {:primary-key? true} uuid?]
    [:name [:string {:max 250}]]
    [:user {:one-to-many? true} :my/user]])

 (def role-tbl
   [:table {:name "role"}
    [:id {:primary-key? true} uuid?]
    [:name [:string {:max 250}]]
    [:user {:one-to-many? true} :my/user]])

 (def system
   (ig/init {:fx/database            {:url "jdbc:postgresql://localhost:64725/test?user=test&password=test"}
             [:fx/entity :my/user]   {:table user-tbl :database (ig/ref :fx/database)}
             [:fx/entity :my/client] {:table client-tbl :database (ig/ref :fx/database)}
             [:fx/entity :my/role]   {:table role-tbl :database (ig/ref :fx/database)}}))

 (def role (get system [:fx/entity :my/role]))
 (def client (get system [:fx/entity :my/client]))
 (def user (get system [:fx/entity :my/user]))

 (def role-id (random-uuid))
 (def client-id (random-uuid))
 (def user-id (random-uuid))

 (repo/create! role {:id   role-id
                     :name "test-role"})

 (repo/create! client {:id   client-id
                       :name "test-client"})

 (repo/create! user {:id     user-id
                     :name   "test"
                     :client client-id
                     :role   role-id})

 (parse-table user-tbl)

 (m/validate
  (:my/user (mr/schemas entities-registry))
  {:id     (random-uuid)
   :name   "test"
   :client (random-uuid)
   :role   (random-uuid)})

 nil)