(ns ddd.service
  (:require [cheshire.core]))


(def ^{:fx/autowire :fx/entity} client
  [:spec {:table "client"}
   [:id {:primary-key? true} uuid?]
   [:name [:string {:max 250}]]
   [:user {:one-to-many? true} ::user]])


(def ^{:fx/autowire :fx/entity} role
  [:spec {:table "role"}
   [:id {:primary-key? true} uuid?]
   [:name [:string {:max 250}]]
   [:user {:one-to-many? true} ::user]])


(def ^{:fx/autowire :fx/entity} user
  [:spec {:table "user"}
   [:id {:primary-key? true} uuid?]
   [:name [:string {:max 250}]]
   [:email [:string {:max 250}]]
   [:client {:many-to-one? true} ::client]
   [:role {:many-to-one? true} ::role]])
