(ns fx.entity
  (:require
   [integrant.core :as ig]))


(defmethod ig/init-key :fx/entity [_ config]
  (println "Entity config" config))
