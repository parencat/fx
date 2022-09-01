(ns fx.repo.pg
  (:require
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as jdbc.rs]
   [honey.sql :as sql]
   [integrant.core :as ig]
   [malli.core :as m]
   [fx.entity]
   [fx.migrate :as migrate]
   [fx.repo :refer [IRepo]])
  (:import
   [java.sql Connection]
   [fx.entity Entity]))


(defn ->columns-names [entity field-name]
  (if (fx.entity/field-prop entity field-name :wrap?)
    [:raw ["\"" (name field-name) "\""]]
    field-name))

(def column-name?
  [:or :keyword
   [:tuple [:= :raw] [:vector :string]]])

(m/=> ->columns-names
  [:=> [:cat fx.entity/entity? :keyword]
   column-name?])


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
      (let [table   (fx.entity/prop entity :table)
            columns (fx.entity/entity-columns entity)
            values  (fx.entity/entity-values entity data)
            query   (-> {:insert-into table
                         :columns     (mapv #(->columns-names entity %) columns)
                         :values      [values]}
                        (sql/format))]
        (jdbc/execute-one! database query
                           {:return-keys true
                            :builder-fn  jdbc.rs/as-unqualified-kebab-maps})))

    (find! [entity {:keys [id]}]
      (let [table (fx.entity/prop entity :table)
            query (-> {:select [:*]
                       :from   (keyword table)
                       :where  [:= :id id]}
                      (sql/format))]
        (jdbc/execute-one! database query
                           {:return-keys true
                            :builder-fn  jdbc.rs/as-unqualified-kebab-maps})))))
