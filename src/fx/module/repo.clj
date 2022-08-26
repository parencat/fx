(ns fx.module.repo
  (:require
   [integrant.core :as ig]
   [duct.core :as duct]))


(defmethod ig/init-key :fx.module/repo [_ {:keys [migrate]
                                           :or   {migrate {}}}]
  (fn [config]
    (duct/merge-configs
     config
     {:fx.repo/migrate migrate
      :fx.repo/table   {:database (ig/ref :fx.database/connection)
                        :migrate  (ig/ref :fx.repo/migrate)
                        :entities (ig/refset :fx/entity)}
      :fx.repo/adapter {:database (ig/ref :fx.database/connection)}})))
