(ns fx.module.database
  (:require
   [integrant.core :as ig]
   [duct.core :as duct]))


(defmethod ig/init-key :fx.module/database [_ {:keys [migrate]
                                               :or   {migrate {}}}]
  (fn [config]
    (duct/merge-configs
     config
     {:fx.database/connection {}
      :fx/migrate             migrate})))
