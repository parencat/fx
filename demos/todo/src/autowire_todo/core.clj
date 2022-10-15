(ns autowire-todo.core
  (:require
   [ring.adapter.jetty :as jetty]
   [next.jdbc :as jdbc]
   [next.jdbc.sql :as jdbc.sql]
   [honey.sql :as sql]
   [compojure.core :as compojure]
   [compojure.route :as route]
   [ring.middleware.json :as json]
   [ring.middleware.params :as params])
  (:import
   [org.eclipse.jetty.server Server]
   [java.sql Connection]))


;; =============================================================================
;; db connection
;; =============================================================================

(defn ^:fx/autowire db-connection [{:keys [db-uri]}]
  (jdbc/get-connection {:connection-uri db-uri
                        :dbtype         "sqlite"}))


(defn ^:fx/autowire close-connection {:fx/halt ::db-connection}
  [^Connection connection]
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

(defn ^:fx/autowire create-table
  [^::db-connection db]
  (jdbc/execute! db create-todo-table-query))


(defn ^:fx/autowire select-all-todos {:fx/wrap true}
  [^::db-connection db _]
  {:status 200
   :body   (jdbc.sql/query db select-all-query)})


(defn ^:fx/autowire update-todo {:fx/wrap true}
  [^::db-connection db {:strs [id done]}]
  {:status 200
   :body   (->> (update-query id (done? done))
                (jdbc/execute! db))})


(defn ^:fx/autowire insert-todo {:fx/wrap true}
  [^::db-connection db {:strs [title]}]
  {:status 200
   :body   (jdbc/execute! db (insert-query title))})


;; =============================================================================
;; routes
;; =============================================================================

(defn ^:fx/autowire routes
  [^::select-all-todos select-all
   ^::update-todo update
   ^::insert-todo insert]
  (compojure/routes
   (compojure/GET "/todos" [] select-all)
   (compojure/POST "/todos" {:keys [query-params]} (insert query-params))
   (compojure/PUT "/todos" {:keys [query-params]} (update query-params))
   (route/not-found "<h1>Page not found</h1>")))


;; =============================================================================
;; http server
;; =============================================================================

(defn ^:fx/autowire server [^::routes app]
  (jetty/run-jetty
   (-> app
       (params/wrap-params)
       (json/wrap-json-response))
   {:port  3000
    :join? false}))


(defn ^:fx/autowire stop-server {:fx/halt ::server}
  [^Server jetty-server]
  (.stop jetty-server))

