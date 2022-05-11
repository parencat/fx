(ns fx.migrate
  (:require
   [integrant.core :as ig]
   [clojure.string :as str]
   [clojure.data :as data]
   [next.jdbc :as jdbc]))


(defmulti get-type-ddl
  (fn [_ {:keys [type]}] type))


(defmethod get-type-ddl uuid? [_ _]
  :uuid)

(defmethod get-type-ddl :uuid [_ _]
  :uuid)

(defmethod get-type-ddl string? [_ _]
  :varchar)

(defmethod get-type-ddl :string [_ {:keys [properties]}]
  (if-some [max (:max properties)]
    [:varchar max]
    :varchar))

(defmethod get-type-ddl :default [all-specs {:keys [type] :as asd}]
  (if-let [ref-spec (and (qualified-keyword? type)
                         (get all-specs type))]
    (let [ref-type (get-in ref-spec [:children 0 :type])]
      (get-type-ddl all-specs {:type ref-type}))

    (throw (ex-info (str "Unknown type for column " type) {:type type}))))


(defn ->ref-table [{:keys [type]}]
  (str/replace (name type) "-ref" ""))


(defn column-ddl [all-specs [column-name column-spec]]
  (let [{:keys [value properties]} column-spec
        column-type (get-type-ddl all-specs value)]
    (cond-> [column-name column-type]

            (not (:optional properties))
            (conj [:not nil])

            (or (:one-to-one? properties) (:many-to-one? properties))
            (conj [:references (->ref-table value)]))))


(defn non-refs [[_column-name {:keys [properties]}]]
  (let [{:keys [one-to-many? many-to-many?]} properties]
    (not (or one-to-many? many-to-many?))))


(defn create-table-ddl [all-specs table-name table-spec]
  (let [columns (->> (:keys table-spec)
                     (filter non-refs)
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


(defn get-changes-ddl [changes])


(defmethod ig/init-key :fx/migrate [_ {:keys [database entities]}]
  (let [db-schema       ()
        entities-schema ()
        [db-changes entities-changes] (data/diff db-schema entities-schema)
        changes-ddl     (get-changes-ddl entities-changes)]
    (doseq [change changes-ddl]
      (jdbc/execute! database change))))







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

 (create-table-ddl @entities-lookup "client" cl-spec)
 (create-table-ddl @entities-lookup "user" user-spec)

 (-> (mr/schema @entities-lookup :fx.entity-test/client-ref)
     (get-in [2 1 1]))

 (def get-type-ddl nil)

 (malli.core/ast [:or uuid? [:map [:id uuid?]]])

 (get-in [:or uuid? [:map [:id uuid?]]] [2 1 1])
 (get-in [:or uuid? [:map [:id uuid?]]] [2 1 1])

 [[:id :uuid [:not nil] [:primary-key]]
  [:name :varchar [:not nil]]
  [:last-name :varchar]]

 nil)
