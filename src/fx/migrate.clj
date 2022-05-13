(ns fx.migrate
  (:require
   [integrant.core :as ig]
   [clojure.string :as str]
   [clojure.data :as data]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [medley.core :as mdl]
   [fx.entity :as entity]
   [honey.sql :as sql])
  (:import [java.sql DatabaseMetaData]
           [clojure.lang PersistentHashSet]))


(defmulti ->type-ddl
  (fn [_ {:keys [type]}] type))

(defmethod ->type-ddl uuid? [_ _]
  :uuid)

(defmethod ->type-ddl :uuid [_ _]
  :uuid)

(defmethod ->type-ddl string? [_ _]
  :varchar)

(defmethod ->type-ddl :string [_ {:keys [properties]}]
  (if-some [max (:max properties)]
    [:varchar max]
    :varchar))

(defmethod ->type-ddl :default [all-specs {:keys [type]}]
  (if-let [ref-spec (and (qualified-keyword? type)
                         (get all-specs type))]
    (let [ref-type (get-in ref-spec [:children 0 :type])]
      (->type-ddl all-specs {:type ref-type}))

    (throw (ex-info (str "Unknown type for column " type) {:type type}))))


(defmulti ->type-name
  (fn [_ {:keys [type]}] type))

(defmethod ->type-name uuid? [_ _]
  "uuid")

(defmethod ->type-name :uuid [_ _]
  "uuid")

(defmethod ->type-name string? [_ _]
  "varchar")

(defmethod ->type-name :string [_ _]
  "varchar")

(defmethod ->type-name :default [all-specs {:keys [type]}]
  (if-let [ref-spec (and (qualified-keyword? type)
                         (get all-specs type))]
    (let [ref-type (get-in ref-spec [:children 0 :type])]
      (->type-name all-specs {:type ref-type}))

    (throw (ex-info (str "Unknown type for column " type) {:type type}))))


(defn ->ref-table [{:keys [type]}]
  (keyword (str/replace (name type) "-ref" "")))


(defn column-ddl [all-specs [column-name column-spec]]
  (let [{:keys [value properties]} column-spec
        column-type (->type-ddl all-specs value)]
    (cond-> [column-name column-type]

            (not (:optional properties))
            (conj [:not nil])

            (or (:one-to-one? properties) (:many-to-one? properties))
            (conj [:references (->ref-table value)])

            (:primary-key? properties)
            (conj [:primary-key]))))


(defn non-refs [{:keys [properties]}]
  (let [{:keys [one-to-many? many-to-many?]} properties]
    (not (or one-to-many? many-to-many?))))


(defn create-table-ddl [all-specs table-name table-spec]
  (let [columns (->> (:keys table-spec)
                     (mdl/filter-vals non-refs)
                     (mapv (partial column-ddl all-specs)))]
    {:create-table table-name
     :with-columns columns}))


(defn create-column-ddl [all-specs table-name column-spec]
  (let [columns ()]
    {:alter-table
     [table-name
      {:add-column columns}
      {:drop-column columns}
      {:modify-column columns}]}))


(defn spec-by-table-name [all-specs table-name]
  (val (mdl/find-first
        (fn [[_ {:keys [properties]}]]
          (= table-name (:table-name properties)))
        all-specs)))


(defn get-table-refs [table-name]
  (let [all-specs  @entity/entities-lookup
        table-spec (spec-by-table-name all-specs table-name)]
    (->> table-spec
         :keys
         vals
         (filter (fn [{:keys [value]}]
                   (qualified-keyword? (:type value))))
         (map (comp ->ref-table :value))
         set)))


(defn has-refs? [dependant dependy]
  (let [table-refs (get-table-refs dependant)]
    (contains? table-refs (keyword dependy))))


(defn by-dependencies [t1 t2]
  (if (has-refs? t1 t2)
    1
    -1))


(defn get-changes-ddl [[db-changes entities-changes common]]
  ;; TODO should we delete tables if user deleted entity?
  (when-not (and (nil? db-changes)
                 (nil? entities-changes))
    (let [all-specs     @entity/entities-lookup
          tables-to-add (filter (fn [table-name]
                                  (and (not (contains? db-changes table-name))
                                       (not (contains? common table-name))))
                                (keys entities-changes))]
      (->> tables-to-add
           (sort by-dependencies)
           (map (fn [table-name]
                  (let [table-spec (spec-by-table-name all-specs table-name)]
                    (-> (create-table-ddl all-specs table-name table-spec)
                        (sql/format)))))))))


(defn get-table-columns [database table]
  (let [^DatabaseMetaData metadata (.getMetaData database)
        columns                    (-> metadata
                                       (.getColumns nil "public" table nil)
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
                                    (contains? pk-set column-name)
                                    (assoc :primary-key true)

                                    (contains? fk-map column-name)
                                    (merge (get fk-map column-name)))]
                (conj acc column')))
            []
            columns)))


(defn extract-db-columns [database entities]
  (let [tables (map #(get-in % [:table :table-name]) entities)]
    (mapcat (partial get-table-columns database) tables)))


(defn db->simple-schema [db-columns]
  (reduce (fn [acc {:keys [table-name column-name type-name nullable primary-key foreign-key references]}]
            (let [column-name' (str/replace column-name "_" "-")
                  table-props  (cond-> {:type type-name :nullable (= nullable 1)}
                                       primary-key
                                       (assoc :primary-key primary-key)
                                       foreign-key
                                       (assoc :foreign-key foreign-key
                                              :references references))]
              (assoc-in acc [table-name column-name'] table-props)))
          {}
          db-columns))


(defn entities->simple-schema [entities]
  (let [all-specs @entity/entities-lookup]
    (reduce (fn [acc entity]
              (let [table-name   (get-in entity [:table :table-name])
                    table-lookup (get-in entity [:table :lookup])
                    table-fields (get-in all-specs [table-lookup :keys])
                    columns      (->> table-fields
                                      (mdl/filter-vals non-refs)
                                      (mdl/map-kv (fn [column-name {:keys [value properties]}]
                                                    [(name column-name)
                                                     (cond-> {:type     (->type-name all-specs value)
                                                              :nullable (true? (:optional properties))}
                                                             (:primary-key? properties)
                                                             (assoc :primary-key true)
                                                             (:foreign-key? properties)
                                                             (assoc :foreign-key true
                                                                    :references (->ref-table value)))])))]
                (assoc acc table-name columns)))
            {}
            entities)))


(defn apply-migrations [{:keys [database ^PersistentHashSet entities]}]
  (let [db-columns             (extract-db-columns database entities)
        db-simple-schema       (db->simple-schema db-columns)
        entities-simple-schema (entities->simple-schema entities)
        changes                (data/diff db-simple-schema entities-simple-schema)
        changes-ddl            (get-changes-ddl changes)]
    (doseq [change changes-ddl]
      (println "runing migration: " change)
      (jdbc/execute! database change))))


(defmethod ig/prep-key :fx/migrate [_ config]
  (merge config
         {:database (ig/ref :fx.database/connection)
          :entities (ig/refset :fx/entity)}))


(defmethod ig/init-key :fx/migrate [_ config]
  (apply-migrations config))







(comment

 (def cl-spec
   {:type :map,
    :keys {:id   {:order      0,
                  :value      {:type uuid?},
                  :properties {:primary-key? true}},
           :name {:order      1,
                  :value      {:type :string, :properties {:max 250}},
                  :properties nil},
           :user {:order      2,
                  :value      {:type :+, :children [{:type :fx.entity-test/user-ref}]},
                  :properties {:one-to-many? true, :optional true}}}})

 (def user-spec
   {:type :map,
    :keys {:id        {:order      0,
                       :value      {:type uuid?},
                       :properties {:primary-key? true}},
           :name      {:order      1,
                       :value      {:type :string, :properties {:max 250}},
                       :properties nil},
           :last-name {:order      2,
                       :value      {:type string?},
                       :properties {:optional true}},
           :client    {:order      3,
                       :value      {:type :fx.entity-test/client-ref},
                       :properties {:many-to-one? true}},
           :role      {:order      4,
                       :value      {:type :fx.entity-test/role-ref},
                       :properties {:many-to-one? true}}}})


 (require '[fx.entity :refer [entities-lookup]])
 (require '[malli.registry :as mr])

 (val (mdl/find-first
       (fn [[_ {:keys [properties]}]]
         (= "client" (:table-name properties)))
       @entities-lookup))

 (create-table-ddl @entities-lookup "client" cl-spec)
 (create-table-ddl @entities-lookup "user" user-spec)

 (-> (mr/schema @entities-lookup :fx.entity-test/client-ref)
     (get-in [2 1 1]))

 (def ->type-ddl nil)

 (malli.core/ast [:or uuid? [:map [:id uuid?]]])

 (get-in [:or uuid? [:map [:id uuid?]]] [2 1 1])
 (get-in [:or uuid? [:map [:id uuid?]]] [2 1 1])

 [[:id :uuid [:not nil] [:primary-key]]
  [:name :varchar [:not nil]]
  [:last-name :varchar]]


 (get-changes-ddl
  '(nil
    {"client" {"id" {:type "uuid", :nullable false, :primary-key true}, "name" {:type "varchar", :nullable false}}}
    {"user" {"id"        {:type "uuid", :nullable false, :primary-key true},
             "name"      {:type "varchar", :nullable false},
             "last-name" {:type "varchar", :nullable true},
             "client"    {:type "uuid", :nullable false, :foreign-key true, :references "client"},
             "role"      {:type "uuid", :nullable false, :foreign-key true, :references "role"}},
     "role" {"id"        {:type "uuid", :nullable false, :primary-key true},
             "name"      {:type "varchar", :nullable false},
             "test-name" {:type "varchar", :nullable true}}}))


 (get-table-refs "user")
 (sort by-dependencies ["role" "user" "client"])

 nil)
