(ns fx.module.entity
  (:require [duct.core :as duct]
            [integrant.core :as ig]))


(defn inject-database [[entity-key entity-val]]
  [entity-key {:table entity-val :database (ig/ref :fx/database)}])


(defmethod ig/init-key :fx.module/entity [_ config]
  (fn [config]
    (let [entity-initial-keys (ig/find-derived config :fx/entity)
          entity-keys         (->> entity-initial-keys
                                   (map inject-database)
                                   (into {}))
          config'             (merge config entity-keys)]
      (duct/merge-configs
       config'
       {:fx/database {}}))))
