(ns duct-todo.core
  (:require
   [next.jdbc :as jdbc]
   [ring.adapter.jetty :as jetty]
   [integrant.core :as ig])
  (:import
   [java.sql Connection]
   [org.eclipse.jetty.server Server]))


(def ^Connection connection
  (jdbc/get-connection
   {:connection-uri "jdbc:sqlite::memory:"
    :dbtype         "sqlite"}))

(.close connection)


(def ^Server server
  (jetty/run-jetty
   (fn [handler]
     "business logic goes here")
   {:port  3000
    :join? false}))

(.stop server)


(defmethod ig/init-key ::connection [_ config]
  (jdbc/get-connection
   {:connection-uri "jdbc:sqlite::memory:"
    :dbtype         "sqlite"}))

(defmethod ig/halt-key! ::connection [connection]
  (.close connection))

(defmethod ig/init-key ::server [_ config]
  (jetty/run-jetty
   (fn [handler]
     "business logic goes here")
   {:port  3000
    :join? false}))

(defmethod ig/halt-key! ::server [^Server server]
  (.stop server))



(defmethod ig/init-key ::connection [_ config]
  (jdbc/get-connection
   {:connection-uri (:database-uri config)
    :dbtype         (:database-type config)}))

(defmethod ig/halt-key! ::connection [connection]
  (.close connection))

(defmethod ig/init-key ::server [_ config]
  (jetty/run-jetty
   (fn [handler]
     "business logic goes here")
   {:port  (:http-port config)
    :join? false}))

(defmethod ig/halt-key! ::server [^Server server]
  (.stop server))



'{:duct.profile/base
  {:duct.core/project-ns duct-todo

   :duct-todo.core/connection
   {:database-uri  #duct/env ["DATABASE_URI" :or "jdbc:sqlite::memory:"]
    :database-type #duct/env ["DATABASE_TYPE" :or "sqlite"]}

   :duct-todo.core/server
   {:http-port #duct/env ["HTTP_PORT" :or 3000 Int]}}}


;{;'{:duct.profile/base
;;  {...
;;
;;   :duct-todo.core/server
;;   {...
;;    :connection #ig/ref :duct-todo.core/connection}}}



(defmethod ig/init-key ::server [_ config]
  (jetty/run-jetty
   (fn [handler]
     (jdbc/execute! (:connection config) ["SELECT * FROM users;"]))
   {:port  (:http-port config)
    :join? false})) }
