(ns fx.module.stub-functions
  (:require [integrant.core :as ig]))


;; add meta to symbol
(def ^:fx/autowire constant-value
  {:connected :ok})


;; add meta to symbol
;; return function
(defn ^:fx/autowire health-check []
  (fn [ctx req]
    {:status :ok}))


;; add meta to defn declaration
(defn db-connection
  {:fx/autowire true}
  []
  (fn [] {:connected :ok}))


;; add meta to defn declaration
;; wire function as halt key
(defn close-connection
  {:fx/autowire true
   :fx/halt     :fx.module.stub-functions/db-connection}
  [connection]
  :closed)


;; specify dependencies as arguments
;; automatically wrap in anonymous function
(defn status
  {:fx/autowire true
   :fx/wrap     true}
  [^:fx.module.stub-functions/db-connection db-connection]
  {:status     :ok
   :connection (db-connection)})


(defn other-handler
  {:fx/autowire true
   :fx/wrap     true}
  [^:fx.module.stub-functions/db-connection db-connection]
  {:status     :fail
   :connection (db-connection)})


(defmethod ig/init-key :fx.module.stub-functions/test-1 [_ config]
  (:component config))


(defmethod ig/init-key :fx.module.stub-functions/test-2 [_ config]
  (:component config))


(def ^{:fx/autowire :fx.module.stub-functions/test-1}
  parent-test-component
  {:component :test-1})


(def ^{:fx/autowire [:fx.module.stub-functions/test-1
                     :fx.module.stub-functions/test-2]}
  multi-parent-test-component
  {:component [:test-1 :test-2]})
