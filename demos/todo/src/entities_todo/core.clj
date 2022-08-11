(ns entities-todo.core
  (:require
   [cheshire.core]
   [fx.entity :as ent]
   [ring.adapter.jetty :as jetty]
   [compojure.core :as compojure]
   [compojure.route :as route]
   [ring.middleware.params :as params]
   [ring.middleware.json :as json])
  (:import [org.eclipse.jetty.server Server]))


(def ^{:fx/autowire :fx/entity} todo
  [:spec {:table "todo"}
   [:id {:primary-key? true} uuid?]
   [:title :string]
   [:list {:many-to-one? true} ::todo-list]])


(def ^{:fx/autowire :fx/entity} todo-list
  [:spec {:table "todo_list"}
   [:id {:primary-key? true} uuid?]
   [:name [:string {:max 250}]]
   [:todos {:one-to-many? true} ::todo]
   [:person {:many-to-one? true} ::person]])


(def ^{:fx/autowire :fx/entity} person
  [:spec {:table "person"}
   [:id {:primary-key? true} uuid?]
   [:name [:string {:max 250}]]
   [:email [:string {:max 250}]]
   [:lists {:one-to-many? true} ::todo-list]])


(defn insert-person-handler
  {:fx/autowire true
   :fx/wrap     true}
  [^::person person {:strs [name email]}]
  {:status  200
   :headers {"Content-Type" "application/json"}
   :body    (ent/create! person {:id    (random-uuid)
                                 :name  name
                                 :email email})})


(defn routes
  {:fx/autowire true}
  [^::insert-person-handler insert-person-handler]
  (compojure/routes
   (compojure/POST "/person" {:keys [query-params]} (insert-person-handler query-params))
   (route/not-found "<h1>Page not found</h1>")))


(defn server
  {:fx/autowire true}
  [^::routes routes]
  (-> routes
      (params/wrap-params)
      (json/wrap-json-response)
      (jetty/run-jetty {:port  3000
                        :join? false})))


(defn stop-server
  {:fx/autowire true
   :fx/halt     ::server}
  [^Server jetty-server]
  (.stop jetty-server))
