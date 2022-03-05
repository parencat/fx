(ns todo.core
  (:require [ring.adapter.jetty :as jetty]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as jdbc.sql]
            [honey.sql :as sql]
            [compojure.core :as compojure]
            [compojure.route :as route]
            [ring.middleware.json :as json]
            [ring.middleware.params :as params])
  (:import [org.eclipse.jetty.server Server]
           [java.sql Connection]))


(defonce ids*
  (atom 1))


(def db-uri
  "jdbc:sqlite::memory:")


(defn close-connection [^Connection connection]
  (.close connection))


(def ^{:fx/autowire true
       :fx/halt     close-connection}
  db-connection
  (jdbc/get-connection {:connection-uri db-uri
                        :dbtype         "sqlite"}))


(def todo-table
  (sql/format {:create-table :todo
               :with-columns
               [[:id :int [:not nil]]
                [:title [:varchar 32]]
                [:done :boolean [:not nil false]]]}))


(defn create-table
  {:fx/autowire true}
  [^:todo.core/db-connection db]
  (jdbc/execute! db todo-table))


(def select-all-todo
  (sql/format {:select :*
               :from   :todo}))


(defn select-all-todo-handler
  {:fx/autowire true
   :fx/wrap     true}
  [^:todo.core/db-connection db _request-params]
  {:status  200
   :headers {"Content-Type" "application/json"}
   :body    (jdbc.sql/query db select-all-todo)})


(defn update-todo [id done]
  (sql/format {:update :todo
               :set    {:done done}
               :where  [:= :id id]}))


(defn update-todo-handler
  {:fx/autowire true
   :fx/wrap     true}
  [^:todo.core/db-connection db {:strs [id done]}]
  {:status  200
   :headers {"Content-Type" "application/json"}
   :body    (jdbc/execute! db (update-todo id (get {"true" true "false" false} done)))})


(defn insert-todo [title]
  (let [id (swap! ids* inc)]
    (sql/format {:insert-into :todo
                 :values      [[id title false]]})))


(defn insert-todo-handler
  {:fx/autowire true
   :fx/wrap     true}
  [^:todo.core/db-connection db {:strs [title]}]
  {:status  200
   :headers {"Content-Type" "application/json"}
   :body    (jdbc/execute! db (insert-todo title))})


(defn routes
  {:fx/autowire true}
  [^:todo.core/select-all-todo-handler select-all-todo-handler
   ^:todo.core/update-todo-handler update-todo-handler
   ^:todo.core/insert-todo-handler insert-todo-handler]
  (compojure/routes
   (compojure/GET "/todos" [] select-all-todo-handler)
   (compojure/POST "/todos" {:keys [query-params]} (insert-todo-handler query-params))
   (compojure/PUT "/todos" {:keys [query-params]} (update-todo-handler query-params))
   (route/not-found "<h1>Page not found</h1>")))


(defn stop-server [^Server jetty-server]
  (.stop jetty-server))


(defn server
  {:fx/autowire true
   :fx/halt     stop-server}
  [^:todo.core/routes app]
  (jetty/run-jetty
   (-> app
       (params/wrap-params)
       (json/wrap-json-response))
   {:port  3000
    :join? false}))

