(ns fx.demo.todo
  (:require [ring.adapter.jetty :as jetty]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as jdbc.sql]
            [honey.sql :as sql]
            [compojure.core :as compojure]
            [compojure.route :as route]
            [ring.middleware.json :as json]
            [ring.middleware.params :as params]))


(defonce ids* (atom 1))


(def db-uri "jdbc:sqlite::memory:")


(def ^:fx.module/autowire db-connection
  (jdbc/get-connection {:connection-uri db-uri
                        :dbtype         "sqlite"}))


(def todo-table (sql/format  {:create-table :todo
                              :with-columns
                              [[:id :int [:not nil]]
                               [:title [:varchar 32]]
                               [:done :boolean [:not nil false]]]}))


(defn create-table
  {:fx.module/autowire true}
  [^:fx.demo.todo/db-connection db]
  (jdbc/execute! db todo-table))


(def select-all-todo (sql/format {:select :*
                                  :from   :todo}))


(defn select-all-todo-handler
  {:fx.module/autowire true}
  [^:fx.demo.todo/db-connection db]
  (fn [_]
    {:status  200
     :headers {"Content-Type" "application/json"}
     :body    (jdbc.sql/query db select-all-todo)}))


(defn update-todo [id done]
  (sql/format {:update :todo
               :set {:done done}
               :where [:= :id id]}))


(defn update-todo-handler
  {:fx.module/autowire true}
  [^:fx.demo.todo/db-connection db]
  (fn [{:strs [id done]}]
    {:status  200
     :headers {"Content-Type" "application/json"}
     :body    (jdbc/execute! db (update-todo id (get {"true" true "false" false} done)))}))


(defn insert-todo [title]
  (sql/format {:insert-into :todo
               :values [[(swap! ids* inc) title false]]}))


(defn insert-todo-handler
  {:fx.module/autowire true}
  [^:fx.demo.todo/db-connection db]
  (fn [{:strs [title]}]
    {:status  200
     :headers {"Content-Type" "application/json"}
     :body    (jdbc/execute! db (insert-todo title))}))


(defn routes
  {:fx.module/autowire true}
  [^:fx.demo.todo/select-all-todo-handler select-all-todo-handler
   ^:fx.demo.todo/update-todo-handler update-todo-handler
   ^:fx.demo.todo/insert-todo-handler insert-todo-handler]
  (compojure/routes
    (compojure/GET "/todos" [] select-all-todo-handler)
    (compojure/POST "/todos" {:keys [query-params]} (insert-todo-handler query-params))
    (compojure/PUT "/todos" {:keys [query-params]} (update-todo-handler query-params))
    (route/not-found "<h1>Page not found</h1>")))


(defn server
  {:fx.module/autowire true}
  [^:fx.demo.todo/routes app]
  (jetty/run-jetty
    (json/wrap-json-response
      (params/wrap-params app))
    {:port 3000
     :join? false}))
