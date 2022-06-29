(ns fx.entity
  (:require
   [integrant.core :as ig]
   [fx.repository :as repo]
   [malli.core :as m]
   [malli.error :as me]
   [malli.registry :as mr]
   [malli.util :as mu]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as jdbc.rs]
   [honey.sql :as sql]
   [medley.core :as mdl])
  (:import [clojure.lang MapEntry]))


;; =============================================================================
;; Entity spec parser
;; =============================================================================

(def EntityRawSpec
  [:schema
   {:registry
    {::entity [:catn
               [:entity keyword?]
               [:properties [:map [:table :string]]]
               [:fields [:* [:schema [:ref ::field]]]]]
     ::field  [:catn
               [:name keyword?]
               [:properties [:? [:map-of keyword? any?]]]
               [:type ::type]]
     ::type   [:altn
               [:fn-schema fn?]
               [:type simple-keyword?]
               [:type-with-props [:tuple keyword? map?]]
               [:entity-ref-key qualified-keyword?]]}}
   ::entity])


(def EntitySpec
  [:schema
   {:registry
    {::entity [:catn
               [:entity keyword?]
               [:properties [:map [:table :string]]]
               [:fields [:* [:schema [:ref ::field]]]]]
     ::field  [:catn
               [:name keyword?]
               [:properties [:? [:map-of keyword? any?]]]
               [:type ::type]]
     ::type   [:altn
               [:fn-schema fn?]
               [:type simple-keyword?]
               [:ref-type [:tuple
                           [:= :entity-ref]
                           [:map
                            [:entity qualified-keyword?]
                            [:entity-ref qualified-keyword?]]]]
               [:type-with-props [:tuple keyword? map?]]]}}
   ::entity])


;; =============================================================================
;; Entities registry
;; =============================================================================

(declare entities-registry)


(def registry*
  (atom (merge
         (m/default-schemas)
         {:spec       (m/-map-schema)
          :entity-ref (m/-simple-schema
                       (fn [{:keys [entity-ref]} _]
                         {:type :entity-ref
                          :pred #(m/validate entity-ref % {:registry entities-registry})}))})))


(def entities-registry
  (mr/mutable-registry registry*))


(defn ->entity-ref [entity]
  (let [entity-ns       (namespace entity)
        entity-name     (name entity)
        entity-ref-name (str entity-name "-ref")]
    (keyword entity-ns entity-ref-name)))


(defn ->entity-ref-schema [schema]
  (let [[pkey pkey-val] (->> (m/entries schema {:registry entities-registry})
                             (filter (fn [[_ v]]
                                       (-> v m/properties :primary-key?)))
                             first)
        pkey-type (-> pkey-val m/children first)]
    [:or pkey-type [:map [pkey pkey-type]]]))


(defn register-entity-ref! [entity schema]
  (swap! registry* assoc (->entity-ref entity) (->entity-ref-schema schema)))


(defn register-entity! [entity schema]
  (register-entity-ref! entity schema)
  (swap! registry* assoc entity schema))


;; =============================================================================
;; Entity helpers
;; =============================================================================

(def required-rel-types
  #{:one-to-one? :many-to-one?})


(def optional-rel-types
  #{:one-to-many? :many-to-many?})


(defn optional-ref? [props]
  (and (some? props)
       (some props optional-rel-types)))


(defn validate-entity [entity data]
  (or (m/validate entity data {:registry entities-registry})
      (throw (ex-info (str "Invalid data for entity " entity)
                      {:error (me/humanize (m/explain entity data {:registry entities-registry}))}))))


(defn entity-entries [entity]
  (let [schema (mr/schema entities-registry entity)]
    (->> (m/entries schema {:registry entities-registry})
         (filter (fn [entry]
                   (if-some [props (-> entry val m/properties)]
                     (not (optional-ref? props))
                     true))))))


(defn entity-columns [entity]
  (->> (entity-entries entity)
       (mapv key)))


(defn entity-values [entity data]
  (let [columns (entity-columns entity)]
    ((apply juxt columns) data)))


(defn ref? [type]
  (let [props (m/properties type)]
    (->> (:entity props)
         (mr/schema entities-registry)
         some?)))


(defn primary-key-schema [entity]
  (let [schema (mr/schema entities-registry entity)]
    (->> (m/entries schema {:registry entities-registry})
         (filter (fn [[_ v]]
                   (-> v m/properties :primary-key?)))
         first)))


(defn schema-type [schema]
  (m/type schema))


(defn properties [schema]
  (m/properties schema))


(defn entry-schema [entry-schema]
  (when-let [schema (some-> entry-schema m/children first)]
    {:type  (m/type schema)
     :props (m/properties schema)}))


(defn entry-schema-table [entry-schema]
  (when-let [schema (some-> entry-schema m/children first)]
    (let [deps-schema (some->> (m/properties schema)
                               :entity
                               (mr/schema entities-registry))]
      (-> deps-schema
          (m/properties {:registry entities-registry})
          :table))))


(defn depends-on? [target dependency]
  (->> (entity-entries target)
       (filter (fn [[_ v]]
                 (let [entry-schema (-> v m/children first)]
                   (and (-> v m/properties :foreign-key?)
                        (= dependency (-> entry-schema m/properties :entity))))))
       not-empty
       boolean))


;; =============================================================================
;; Entity implementation
;; =============================================================================

(defrecord Entity [database entity table]
  repo/PRepository
  (create! [_ entity-map]
    (validate-entity entity entity-map)
    (let [columns (entity-columns entity)
          values  (entity-values entity entity-map)
          query   (-> {:insert-into table}
                      (assoc :columns columns)
                      (assoc :values [values])
                      (sql/format))]
      (jdbc/execute-one! database query
                         {:return-keys true
                          :builder-fn  jdbc.rs/as-unqualified-kebab-maps})))

  (update! [_])
  (find! [_])
  (find-all! [_])
  (delete! [_]))


(def entity?
  (m/-simple-schema
   {:type :entity/instance
    :pred #(instance? Entity %)}))


(def schema?
  (m/-simple-schema
   {:type :entity/schema
    :pred m/schema?}))


;; =============================================================================
;; Entity constructor
;; =============================================================================

(defn ->ref-spec-type [entity]
  (MapEntry. :ref-type [:entity-ref
                        {:entity     entity
                         :entity-ref (->entity-ref entity)}]))


(defn prepare-spec [spec]
  (let [parsed-spec (m/parse EntityRawSpec spec)

        {:keys [fields deps]}
        (reduce (fn [{:keys [fields deps]} {:keys [properties type] :as field}]
                  (let [ref?          (= :entity-ref-key (-> type first))
                        optional-prop (:optional? properties)
                        optional-ref  (optional-ref? properties)
                        field'        (cond-> field
                                              ref?
                                              (-> (assoc :type (-> type second ->ref-spec-type))
                                                  (assoc-in [:properties :foreign-key?] true))

                                              (or optional-prop optional-ref)
                                              (-> (assoc-in [:properties :optional] true)
                                                  (mdl/dissoc-in [:properties :optional?])))]
                    {:fields (conj fields field')
                     :deps   (cond-> deps
                                     (and ref? (not optional-ref))
                                     (conj (second type)))}))
                {} (:fields parsed-spec))]
    {:deps deps
     :spec (->> (assoc parsed-spec :fields fields)
                (m/unparse EntitySpec))}))


(defn create-entity [entity config]
  (let [{:keys [spec database]} config]
    (let [table (-> (m/properties spec {:registry entities-registry})
                    :table)]
      (register-entity! entity spec)
      (->Entity database entity table))))


;; =============================================================================
;; Duct integration
;; =============================================================================

(defmethod ig/prep-key :fx/entity [entity-key raw-spec]
  (let [entity      (if (vector? entity-key) (second entity-key) entity-key)
        valid-spec? (m/validate EntityRawSpec raw-spec)]

    (when-not valid-spec?
      (throw (ex-info (str "Invalid spec schema for entity " entity)
                      {:error (me/humanize (m/explain EntityRawSpec raw-spec))})))

    (let [{:keys [spec deps]} (prepare-spec raw-spec)]
      (reduce (fn [acc dep-entity]
                (assoc acc dep-entity (ig/ref [:fx/entity dep-entity])))
              {:spec     spec
               :database (ig/ref :fx.database/connection)}
              deps))))


(defmethod ig/init-key :fx/entity [entity-key config]
  (let [entity-name (if (vector? entity-key) (second entity-key) entity-key)]
    (create-entity entity-name config)))
