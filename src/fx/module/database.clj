(ns fx.module.database
  (:require
   [duct.core :as duct]
   [integrant.core :as ig]))


(defmethod ig/init-key :fx.module/database [_ module-config]
  (fn [config]
    ;; TODO setup all required jdbc config options here
    (let [db-config {}]
      (duct/merge-configs
       config
       {:fx/database db-config}))))
