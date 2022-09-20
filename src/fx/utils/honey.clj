(ns fx.utils.honey
  (:require
   [honey.sql :as sql]
   [clojure.string :as str]))


;; Postgres doesn't support :modify-column clause
(sql/register-clause! :alter-column :modify-column :modify-column)
(sql/register-clause! :add-constraint :modify-column :modify-column)
(sql/register-clause! :drop-constraint :modify-column :modify-column)


(sql/register-fn!
 :quote
 (fn [_ [kw]]
   [(str "\"" (name kw) "\"")]))


(defn format-simple-expr [e context]
  (let [[sql & params] (sql/format-expr e)]
    (when (seq params)
      (throw (ex-info (str "parameters are not accepted in " context)
                      {:expr e :params params})))
    sql))


(defn- format-single-column [xs]
  (str/join " " (cons (format-simple-expr (first xs) "column operation")
                      (map #(format-simple-expr % "column operation")
                           (rest xs)))))


(defn- format-table-columns [_ xs]
  [(str "("
        (str/join ", " (map #'format-single-column xs))
        ")")])


(sql/register-clause! :with-columns-raw format-table-columns nil)


(defn format-raw-update [_ x]
  [(str "UPDATE " (first (sql/format-expr [:inline x])))])


(sql/register-clause! :update-raw format-raw-update :update)
