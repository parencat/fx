(ns fx.module.http-server
  (:require [integrant.core :as ig]))


(defmethod ig/init-key :http-server/listener [_ _])

(defmethod ig/init-key :http-server/handler [_ _]
  (println "I'm parent key!"))
