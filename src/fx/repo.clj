(ns fx.repo
  (:require
   [fx.entity]
   [honey.sql :as sql]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as jdbc.rs]
   [integrant.core :as ig]
   [fx.migrate :as migrate]
   [malli.core :as m])
  (:import
   [java.sql Connection]
   [fx.entity Entity]))


(defprotocol IRepo
  (save! [entity data])
  (update! [entity data options])
  (delete! [entity options])
  (find! [entity options]))


(defmethod ig/init-key :fx.repo/migrate [_ config]
  (merge {:strategy :none} config))


(defmethod ig/init-key :fx.repo/table [_ {:keys [migrate] :as config}]
  (let [{:keys [rollback-migrations]}
        (case (:strategy migrate)
          (:update :update-drop) (migrate/apply-migrations! config)
          :store (migrate/store-migrations! config)
          :validate (migrate/validate-schema! config)
          nil)]
    (assoc config :rollback-migrations rollback-migrations)))


(defmethod ig/halt-key! :fx.repo/table [_ {:keys [^Connection database migrate rollback-migrations]}]
  (when (= (:strategy migrate) :update-drop)
    (migrate/drop-migrations! database rollback-migrations)))


(defmethod ig/init-key :fx.repo/adapter [_ {:keys [database]}]
  (extend-protocol IRepo
    Entity
    (save! [entity data]
      (let [table   (fx.entity/prop entity :table)
            columns (fx.entity/entity-columns entity)
            values  (fx.entity/entity-values entity data)
            query   (-> {:insert-into table
                         :columns     columns
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
