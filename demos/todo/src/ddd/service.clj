(ns ddd.service
  (:require [cheshire.core]))


(def ^{:fx/autowire :fx/entity} client
  [:table {:name "client"}
   [:id {:primary-key? true} uuid?]
   [:name string?]])


(def ^{:fx/autowire :fx/entity} role
  [:table {:name "role"}
   [:id {:primary-key? true} uuid?]
   [:name string?]
   [:user {:has-many? true} :ddd.service/user]])


(def ^{:fx/autowire :fx/entity} user
  [:table {:name "user"}
   [:id {:primary-key? true} uuid?]
   [:name {:type "text"} string?]
   [:last-name {:optional? true} string?]
   [:client {:has-one? true} :ddd.service/client]
   [:role {:has-many? true} :ddd.service/role]])



(comment

 ;; inject to autowired functions
 ;; cross entities dependencies

 ;; create ddl from entities
 ;; ddl changes strategies
 ;; apply ddl changes to the DB

 ;; how to configure DB driver

 ;; implement common repo protocols (find, find-all, save, delete)
 ;; lazy data fetching

 ;; tests

 nil)

