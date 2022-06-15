(ns ddd.service
  (:require [cheshire.core]))


(def ^{:fx/autowire :fx/entity} client
  [:spec {:table "client"}
   [:id {:primary-key? true} uuid?]
   [:name string?]])


(def ^{:fx/autowire :fx/entity} role
  [:spec {:table "role"}
   [:id {:primary-key? true} uuid?]
   [:name string?]
   [:user {:has-many? true} :ddd.service/user]])


(def ^{:fx/autowire :fx/entity} user
  [:spec {:table "user"}
   [:id {:primary-key? true} uuid?]
   [:name {:type "text"} string?]
   [:last-name {:optional? true} string?]
   [:client {:has-one? true} :ddd.service/client]
   [:role {:has-many? true} :ddd.service/role]])



(comment

 ;; inject to autowired functions +
 ;; cross entities dependencies +

 ;; change autowire to always use find-derived for component dependencies

 ;; parse table definition

 ;; create ddl from entities
 ;; ddl changes strategies
 ;; apply ddl changes to the DB

 ;; finish Entity record implementation

 ;; how to configure DB driver

 ;; implement common repo protocols (find, find-all, save, delete)
 ;; lazy data fetching (optional)

 ;; tests

 nil)

