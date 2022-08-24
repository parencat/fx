(ns fx.table
  (:require
   [integrant.core :as ig]
   [fx.utils.common :as uc]
   [fx.migrate :as migrate])
  (:import
   [java.sql Connection]))


(defmethod ig/init-key :fx.table/migrate [_ config]
  (merge {:strategy :none} config))


(defmethod ig/prep-key :fx/table [entity-key _]
  (let [entity-type (uc/entity-key->entity-type entity-key)]
    {:database (ig/ref :fx.database/connection)
     :migrate  (ig/ref :fx.table/migrate)
     :entity   (ig/ref entity-type)}))


(defmethod ig/init-key :fx/table [_ {:keys [migrate] :as config}]
  (let [migration-result (case (:strategy migrate)
                           (:update :update-drop) (migrate/apply-migrations! config)
                           :store (migrate/store-migrations! config)
                           :validate (migrate/validate-schema! config)
                           nil)]
    (merge config migration-result)))


(defmethod ig/halt-key! :fx/table [_ {:keys [^Connection database migrate rollback-migrations]}]
  (when (= (:strategy migrate) :update-drop)
    (migrate/drop-migrations! database rollback-migrations)))
