(ns fx.entity
  (:refer-clojure
   :exclude [cast])
  (:require
   [integrant.core :as ig]
   [malli.core :as m]
   [malli.error :as me]
   [malli.registry :as mr]
   [malli.transform :as mt]
   [fx.utils.common :as uc])
  (:import
   [clojure.lang MapEntry]))


;; =============================================================================
;; Entity spec parser
;; =============================================================================

(def EntityRawSpec
  "Recursive malli schema to parse provided by user entities schemas e.g.
   [:spec {:table \"client\"}
     [:id {:identity true} uuid?]
     [:name [:string {:max 250}]]
     [:user {:rel-type :one-to-many} :fx.entity-test/user]]"
  [:schema
   {:registry
    {::entity [:catn
               [:entity :keyword]
               [:properties [:? map?]]
               [:fields [:* [:schema [:ref ::field]]]]]
     ::field  [:catn
               [:name :keyword]
               [:properties [:? [:map-of :keyword :any]]]
               [:type ::type]]
     ::type   [:altn
               [:fn-schema fn?]
               [:type simple-keyword?]
               [:type-with-props [:tuple :keyword map?]]
               [:entity-ref-key :qualified-keyword]]}}
   ::entity])


(def entity-raw-spec?
  (m/-simple-schema
   {:type :entity/raw-spec
    :pred #(m/validate EntityRawSpec %)}))


(def EntitySpec
  "Recursive malli schema to unparse provided by user entities schemas to internal schema representation"
  [:schema
   {:registry
    {::entity [:catn
               [:entity :keyword]
               [:properties [:? map?]]
               [:fields [:* [:schema [:ref ::field]]]]]
     ::field  [:catn
               [:name :keyword]
               [:properties [:? [:map-of :keyword :any]]]
               [:type ::type]]
     ::type   [:altn
               [:fn-schema fn?]
               [:type simple-keyword?]
               [:ref-type [:tuple
                           [:= :entity-ref]
                           [:map
                            [:entity :qualified-keyword]
                            [:entity-ref :qualified-keyword]]]]
               [:type-with-props [:tuple :keyword map?]]]}}
   ::entity])


(def entity-spec?
  (m/-simple-schema
   {:type :entity/spec
    :pred #(m/validate EntitySpec %)}))


;; =============================================================================
;; Entities registry
;; =============================================================================

(declare entities-registry)


(defn string->identity [ref x]
  (if (string? x)
    (m/decode ref x {:registry entities-registry} mt/string-transformer)
    x))


(def sequential-relations
  #{:one-to-many :many-to-many})


(def entity-ref-schema
  (m/-simple-schema
   (fn [{:keys [entity-ref rel-type]} _children]
     {:type            :entity-ref
      :pred            (fn [x]
                         (if (contains? sequential-relations rel-type)
                           (m/validate [:sequential entity-ref] x {:registry entities-registry})
                           (m/validate entity-ref x {:registry entities-registry})))
      :type-properties {:decode/string #(string->identity entity-ref %)
                        :error/fn      (fn [error _]
                                         (if (contains? sequential-relations rel-type)
                                           (format "should be a sequence of items matching %s spec, was %s"
                                                   entity-ref (:value error))
                                           (format "should match entity spec for %s, was %s"
                                                   entity-ref (:value error))))}})))

(def registry*
  "Atom to hold malli schemas"
  (atom (merge
         (m/default-schemas)
         {:spec       (m/-map-schema)
          :entity-ref entity-ref-schema})))


(def entities-registry
  "Mutable malli registry to hold all defined by user entities  + some utility schemas"
  (mr/mutable-registry registry*))


(defn ->entity-ref
  "Returns the entity ref type for given entity type e.g.
   :my-cool/entity -> :my-cool/entity-ref"
  [entity-type]
  (let [entity-ns       (namespace entity-type)
        entity-name     (name entity-type)
        entity-ref-name (str entity-name "-ref")]
    (keyword entity-ns entity-ref-name)))

(m/=> ->entity-ref
  [:=> [:cat :qualified-keyword]
   :qualified-keyword])


(defn ->entity-ref-schema
  "Will return a simplified malli schema for a given entity
   Basically wil try to identify a type of primary key field"
  [schema]
  (let [[pkey pkey-val] (->> (m/entries schema {:registry entities-registry})
                             (filter (fn [[_ v]]
                                       (-> v m/properties :identity)))
                             first)
        pkey-type (-> pkey-val m/children first)]
    [:or pkey-type [:map [pkey pkey-type]]]))

(m/=> ->entity-ref-schema
  [:=> [:cat entity-spec?]
   [:tuple [:= :or] :any
    [:tuple [:= :map] [:tuple :any :any]]]])


(defn register-entity-ref!
  "Adds the entity ref schema to the global registry"
  [entity-type schema]
  (swap! registry* assoc (->entity-ref entity-type) (->entity-ref-schema schema)))

(m/=> register-entity-ref!
  [:=> [:cat :qualified-keyword entity-spec?]
   :any])


(defn register-entity!
  "Adds entity and its reference schemas to the global registry"
  [entity-type schema]
  (register-entity-ref! entity-type schema)
  (swap! registry* assoc entity-type schema))

(m/=> register-entity!
  [:=> [:cat :qualified-keyword entity-spec?]
   :any])


;; =============================================================================
;; Entity implementation
;; =============================================================================

(defrecord Entity [type])


(def entity?
  (m/-simple-schema
   {:type :entity/instance
    :pred #(instance? Entity %)}))


(def schema?
  (m/-simple-schema
   {:type :entity/schema
    :pred m/schema?}))


(def entity-field-schema?
  (m/-simple-schema
   {:type :entity/field-schema
    :pred #(and (map-entry? %)
                (-> % val m/schema?))}))


(def field-schema?
  (m/-simple-schema
   {:type :entity/field-val-schema
    :pred #(= (m/type %) ::m/val)}))


;; =============================================================================
;; Entity helpers
;; =============================================================================

(def required-rel-types
  #{:one-to-one :many-to-one})


(defn required-ref?
  "Check if reference field is required"
  [props]
  (-> (and (some? props)
           (contains? required-rel-types (:rel-type props)))
      boolean))

(m/=> required-ref?
  [:=> [:cat [:maybe map?]]
   :boolean])


(def optional-rel-types
  #{:one-to-many :many-to-many})


(defn optional-ref?
  "Check if reference field is optional"
  [props]
  (-> (and (some? props)
           (contains? optional-rel-types (:rel-type props)))
      boolean))

(m/=> optional-ref?
  [:=> [:cat [:maybe map?]]
   :boolean])


(defn valid-entity?
  "Check if data is aligned with entity spec"
  [entity data]
  (let [entity-type (:type entity)]
    (or (m/validate entity-type data {:registry entities-registry})
        (throw (ex-info (str "Invalid data for entity " entity-type)
                        {:error (me/humanize (m/explain entity-type data {:registry entities-registry}))})))))

(m/=> valid-entity?
  [:=> [:cat entity? map?]
   :boolean])


(defn entity-fields
  "Return a list of entity fields specs (map-entries)"
  [entity]
  (let [entity-type (:type entity)
        schema      (mr/schema entities-registry entity-type)]
    (m/entries schema {:registry entities-registry})))

(m/=> entity-fields
  [:=> [:cat entity?]
   [:sequential entity-field-schema?]])


(defn entity-field
  "Return a field spec (map-entries)"
  [entity field-key]
  (let [entity-type (:type entity)
        schema      (mr/schema entities-registry entity-type)]
    (->> (m/entries schema {:registry entities-registry})
         (filter (fn [entry]
                   (= (key entry) field-key)))
         first)))

(m/=> entity-field
  [:=> [:cat entity? :keyword]
   [:maybe entity-field-schema?]])


(defn entity-columns
  "Return a list of entity fields names (keywords)"
  [entity]
  (->> (entity-fields entity)
       (mapv key)))

(m/=> entity-columns
  [:=> [:cat entity?]
   [:sequential :keyword]])


(defn ref?
  "Check if type is a reference to another entity"
  [val-schema]
  (let [props (-> val-schema m/children first m/properties)]
    (->> (:entity props)
         (mr/schema entities-registry)
         some?)))

(m/=> ref?
  [:=> [:cat schema?]
   :boolean])


(defn ident-field-schema
  "Get the spec of a field which is marked as identity field"
  [entity]
  (let [schema (mr/schema entities-registry (or (:type entity) entity))]
    (->> (m/entries schema {:registry entities-registry})
         (filter (fn [[_ v]]
                   (-> v m/properties :identity)))
         first)))

(m/=> ident-field-schema
  [:=> [:cat [:or entity? :qualified-keyword]]
   entity-field-schema?])


(defn ref-field-schema
  "Get the field schema which is a reference to the specified type"
  [entity target-entity]
  (let [schema (mr/schema entities-registry (or (:type entity) entity))
        target (or (:type target-entity) target-entity)]
    (->> (m/entries schema {:registry entities-registry})
         (filter (fn [[_ v]]
                   (= (some-> v m/children first m/properties :entity)
                      target)))
         first)))

(m/=> ref-field-schema
  [:=> [:cat [:or entity? :qualified-keyword] [:or entity? :qualified-keyword]]
   [:maybe entity-field-schema?]])


(defn schema-type
  "Get the schema type"
  [schema]
  (m/type schema))

(m/=> schema-type
  [:=> [:cat schema?]
   :keyword])


(defn properties
  "Get schema properties map if presented"
  [schema]
  (m/properties schema))

(m/=> properties
  [:=> [:cat schema?]
   [:maybe map?]])


(defn prop
  "Get entity property under specified key"
  [entity prop-key]
  (let [entity-type (or (:type entity) entity)]
    (-> (mr/schema entities-registry entity-type)
        (m/properties {:registry entities-registry})
        (get prop-key))))

(m/=> prop
  [:=> [:cat [:or entity? :qualified-keyword] :keyword]
   :any])


(defn field-schema
  "Get the simplified definition of the field spec"
  [field-schema]
  (when-let [schema (some-> field-schema m/children first)]
    {:type  (m/type schema)
     :props (m/properties schema)}))

(m/=> field-schema
  [:=> [:cat field-schema?]
   [:map
    [:type [:or :symbol :keyword]]
    [:props [:maybe map?]]]])


(defn entity-field-prop
  "Get field property value of the given entity"
  [entity field-key prop-key]
  (some-> (entity-field entity field-key)
          val
          (m/properties {:registry entities-registry})
          (get prop-key)))


(defn ref-entity-prop
  "Get any property value from referenced entity by given ref field"
  [field-schema prop-key]
  (when-let [schema (some-> field-schema m/children first)]
    (let [deps-schema (some->> (m/properties schema)
                               :entity
                               (mr/schema entities-registry))]
      (-> deps-schema
          (m/properties {:registry entities-registry})
          (get prop-key)))))

(m/=> ref-entity-prop
  [:=> [:cat field-schema? :keyword]
   :any])


(defn field-type-prop
  "Get any field type property value"
  [field-schema prop-key]
  (when-let [schema (some-> field-schema m/children first)]
    (-> schema
        (m/properties {:registry entities-registry})
        (get prop-key))))

(m/=> field-type-prop
  [:=> [:cat field-schema? :keyword]
   :any])


(defn field-prop
  "Get field property value"
  [field-schema prop-key]
  (some-> field-schema
          (m/properties {:registry entities-registry})
          (get prop-key)))

(m/=> field-prop
  [:=> [:cat field-schema? :keyword]
   :any])


(defn depends-on?
  "Check if one entity depends on the other as foreign tables in SQL"
  [target dependency]
  (->> (entity-fields target)
       (filter (fn [[_ v]]
                 (let [entry-schema (-> v m/children first)
                       props        (m/properties v)]
                   (and (:reference props)
                        (not (optional-ref? props))
                        (= (:type dependency)
                           (-> entry-schema m/properties :entity))))))
       not-empty
       boolean))

(m/=> depends-on?
  [:=> [:cat entity? entity?]
   :boolean])


(defn cast
  "Cast data fields according to entity schema"
  [entity data]
  (let [schema (if (keyword? entity)
                 (mr/schema entities-registry entity)
                 (mr/schema entities-registry (:type entity)))]
    (m/decode schema data {:registry entities-registry} mt/string-transformer)))

(m/=> cast
  [:=> [:cat [:or entity? :qualified-keyword] map?]
   map?])


;; =============================================================================
;; Entity constructor
;; =============================================================================

(defn ->ref-spec-type [entity rel-type]
  (MapEntry. :ref-type [:entity-ref
                        {:entity     entity
                         :rel-type   rel-type
                         :entity-ref (->entity-ref entity)}]))


(defn prepare-spec
  "Transform the user defined entity spec to something more understandable for malli.
   Original specs will cause errors due to cress spec references"
  [spec]
  (let [parsed-spec (m/parse EntityRawSpec spec)

        {:keys [fields deps]}
        (reduce (fn [{:keys [fields deps]} {:keys [properties type] :as field}]
                  (let [ref?         (= :entity-ref-key (-> type first))
                        optional-ref (optional-ref? properties)
                        field'       (cond-> field
                                             ref?
                                             (-> (assoc :type (->ref-spec-type (-> type second) (:rel-type properties)))
                                                 (assoc-in [:properties :reference] true)))]
                    {:fields (conj fields field')
                     :deps   (cond-> deps
                                     (and ref? (not optional-ref))
                                     (conj (second type)))}))
                {} (:fields parsed-spec))]
    {:deps deps
     :spec (->> (assoc parsed-spec :fields fields)
                (m/unparse EntitySpec))}))

(m/=> prepare-spec
  [:=> [:cat entity-raw-spec?]
   [:map
    [:deps [:maybe [:sequential :qualified-keyword]]]
    [:spec entity-spec?]]])


(defn create-entity
  "Constructor function for entities"
  [entity-type spec]
  (register-entity! entity-type spec)
  (->Entity entity-type))

(m/=> create-entity
  [:=> [:cat :qualified-keyword entity-spec?]
   entity?])


;; =============================================================================
;; Duct integration
;; =============================================================================

(defmethod ig/prep-key :fx/entity [entity-key raw-spec]
  (let [valid-spec? (m/validate EntityRawSpec raw-spec)]

    (when-not valid-spec?
      (let [entity-type (uc/entity-key->entity-type entity-key)]
        (throw (ex-info (str "Invalid spec schema for entity " entity-type)
                        {:error (me/humanize (m/explain EntityRawSpec raw-spec))}))))

    (let [{:keys [spec deps]} (prepare-spec raw-spec)]
      (reduce (fn [acc dep-entity]
                (assoc acc dep-entity (ig/ref [:fx/entity dep-entity])))
              {:spec spec}
              deps))))


(defmethod ig/init-key :fx/entity [entity-key {:keys [spec]}]
  (let [entity-type (uc/entity-key->entity-type entity-key)]
    (create-entity entity-type spec)))
