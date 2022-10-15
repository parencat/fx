(ns autowire-todo.core-ig
  (:require
   [ring.adapter.jetty :as jetty]
   [next.jdbc :as jdbc]
   [next.jdbc.sql :as jdbc.sql]
   [honey.sql :as sql]
   [compojure.core :as compojure]
   [compojure.route :as route]
   [ring.middleware.json :as json]
   [ring.middleware.params :as params]
   [integrant.core :as ig])
  (:import
   [org.eclipse.jetty.server Server]
   [java.sql Connection]))


;; =============================================================================
;; db connection
;; =============================================================================

(defmethod ig/init-key :todo.core/db-connection [_ {:keys [db-uri]}]
  (jdbc/get-connection {:connection-uri db-uri
                        :dbtype         "sqlite"}))


(defmethod ig/halt-key! :todo.core/db-connection [_ ^Connection connection]
  (.close connection))


;; =============================================================================
;; business logic
;; =============================================================================

(def create-todo-table-query
  (sql/format {:create-table :todo
               :with-columns
               [[:id :int [:not nil]]
                [:title [:varchar 32]]
                [:done :boolean [:not nil false]]]}))


(defn insert-query [title]
  (let [id (swap! ids* inc)]
    (sql/format {:insert-into :todo
                 :values      [[id title false]]})))


(def select-all-query
  (sql/format {:select :*
               :from   :todo}))


(defn update-query [id done]
  (sql/format {:update :todo
               :set    {:done done}
               :where  [:= :id id]}))


(defn done? [status]
  (get {"true" true "false" false} status))


;; =============================================================================
;; http handlers
;; =============================================================================

(defmethod ig/init-key :todo.core/create-table [_ {:keys [db]}]
  (jdbc/execute! db create-todo-table-query))


(defmethod ig/init-key :todo.core/select-all-todos [_ {:keys [db]}]
  (fn [_]
    {:status 200
     :body   (jdbc.sql/query db select-all-query)}))


(defmethod ig/init-key :todo.core/update-todo [_ {:keys [db]}]
  (fn [{:strs [id done]}]
    {:status 200
     :body   (->> (update-query id (done? done))
                  (jdbc/execute! db))}))


(defmethod ig/init-key :todo.core/insert-todo [_ {:keys [db]}]
  (fn [{:strs [title]}]
    {:status 200
     :body   (jdbc/execute! db (insert-query title))}))


;; =============================================================================
;; routes
;; =============================================================================

(defmethod ig/init-key :todo.core/routes [_ {:keys [select-all update insert]}]
  (compojure/routes
   (compojure/GET "/todos" [] select-all)
   (compojure/POST "/todos" {:keys [query-params]} (insert query-params))
   (compojure/PUT "/todos" {:keys [query-params]} (update query-params))
   (route/not-found "<h1>Page not found</h1>")))


;; =============================================================================
;; http server
;; =============================================================================

(defmethod ig/init-key :todo.core/server [_ {:keys [app]}]
  (jetty/run-jetty
   (-> app
       (params/wrap-params)
       (json/wrap-json-response))
   {:port  3000
    :join? false}))


(defmethod ig/halt-key! :todo.core/server [_ ^Server jetty-server]
  (.stop jetty-server))

