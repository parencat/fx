(ns fx.module.stub-functions
  (:require
   [integrant.core :as ig]))


;; autowire components by adding :fx/autowire metadata key to vars
;; value of that var will be used as a system config for that integrant key
(def ^:fx/autowire constant-value
  {:connected :ok})


;; autowire components by adding :fx/autowire metadata key to the symbol (function name)
;; simple function will be invoked during system initialization
;; returning value will be used as a system config for that integrant key
(defn ^:fx/autowire simple-function-value []
  {:status :ok})


;; you can return anonymous functions for the later use
(defn ^:fx/autowire health-check []
  (fn [ctx req]
    {:status :ok}))


;; a shorthand syntax for returning an anonymous function is to use :fx/wrap
(defn ^:fx/autowire ^:fx/wrap wrapped-health-check []
  {:status :ok})


;; also, you can use function metadata instead of symbol metadata
(defn db-connection
  {:fx/autowire true}
  []
  (fn [] {:connected :ok}))


;; you can hook to the halt phase by using :fx/halt key with the name of component
(defn close-connection
  {:fx/autowire true :fx/halt ::db-connection}
  [connection]
  :closed)


;; specify component dependencies as arguments metadata
(defn ^:fx/autowire ^:fx/wrap status [^::db-connection db-connection]
  {:status     :ok
   :connection (db-connection)})


(defn ^:fx/autowire ^:fx/wrap other-handler [^::db-connection db-connection]
  {:status     :fail
   :connection (db-connection)})



;; define a simple component (could be custom or provided by third-party libraries)
(defmethod ig/init-key ::test [_ config]
  (:component config))

;; create a new instance of a ::test component (aka parent child hierarchy)
;; component key will look like [:fx.module.stub-functions/test :fx.module.stub-functions/child-test-component]
;; it doesn't make sense to define such component as functions
;; will work only for vars
(def ^{:fx/autowire ::test} child-test-component
  {:component :test})


(defn ^:fx/autowire fn-with-config [config]
  config)


(defn ^:fx/autowire fn-with-deps-and-config [^::db-connection db-connection config]
  [db-connection config])


(defn ^:fx/autowire ^:fx/wrap fn-with-deps-and-config-and-args [^::db-connection db-connection config args]
  [db-connection config args])
