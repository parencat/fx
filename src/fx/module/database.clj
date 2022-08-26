(ns fx.module.database
  (:require
   [integrant.core :as ig]
   [duct.core :as duct]))


(defmethod ig/init-key :fx.module/database [_ _]
  (fn [config]
    (duct/merge-configs
     config
     {:fx.database/connection {}})))
