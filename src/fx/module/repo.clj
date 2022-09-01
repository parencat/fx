(ns fx.module.repo
  (:require
   [integrant.core :as ig]
   [duct.core :as duct]))


(defn pg-adapter-config [migrate adapter]
  (let [{:keys            [strategy]
         migrate-enabled? :enabled?
         migrate-key      :key
         :or              {migrate-enabled? true
                           migrate-key      :fx.repo.pg/migrate}} migrate
        {adapter-key :key
         :or         {adapter-key :fx.repo.pg/adapter}} adapter]
    (cond-> {}
            migrate-enabled?
            (assoc [migrate-key :fx.repo/migrate]
                   {:strategy strategy
                    :database (ig/ref :fx.database/connection)
                    :entities (ig/refset :fx/entity)})

            :always
            (assoc [adapter-key :fx.repo/adapter]
                   {:database (ig/ref :fx.database/connection)}))))


(defmethod ig/init-key :fx.module/repo [_ {:keys [migrate adapter]}]
  (fn [config]
    (duct/merge-configs
     config
     (pg-adapter-config migrate adapter))))
