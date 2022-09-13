(ns fx.utils.honey
  (:require
   [honey.sql :as sql]))


;; Postgres doesn't support :modify-column clause
(sql/register-clause! :alter-column :modify-column :modify-column)
(sql/register-clause! :add-constraint :modify-column :modify-column)
(sql/register-clause! :drop-constraint :modify-column :modify-column)


(sql/register-fn!
 :quote
 (fn [_ [kw]]
   [(str "\"" (name kw) "\"")]))
