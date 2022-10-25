(ns fx.utils.honey
  (:require
   [honey.sql :as sql]
   [clojure.string :as str]))


;; Postgres doesn't support :modify-column clause
(sql/register-clause! :alter-column :modify-column :modify-column)


(sql/register-fn!
 :quote
 (fn [_ [kw]]
   [(str "\"" (name kw) "\"")]))


(sql/register-fn!
 :cascade
 (fn [_ _]
   ["ON DELETE CASCADE"]))


(sql/register-fn!
 :no-action
 (fn [_ _]
   ["ON DELETE NO ACTIONL"]))


(sql/register-fn!
 :double-precision
 (fn [_ _]
   ["double precision"]))


(sql/register-fn!
 :interval
 (fn [_ [field]]
   (if (some? field)
     [(str "interval " field)]
     ["interval"])))


(sql/register-fn!
 :timestamp-tz
 (fn [_ _]
   ["timestamp with time zone"]))


(sql/register-fn!
 :time-tz
 (fn [_ _]
   ["time with time zone"]))


(sql/register-fn!
 :array
 (fn [_ [type]]
   [(str type "[]")]))


(sql/register-fn!
 :named-constraint
 (fn [_ [name constraint]]
   [(str "CONSTRAINT " name " " (first (sql/format-expr constraint)))]))


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


(defn- format-add-column [_ spec]
  (if (contains? #{:if-not-exists 'if-not-exists} (last spec))
    [(str "ADD COLUMN " (sql/sql-kw :if-not-exists) " " (format-single-column (butlast spec)))]
    [(str "ADD COLUMN " (format-single-column spec))]))


(sql/register-clause! :add-column-raw format-add-column :add-column)


(defn- format-alter-column [_ spec]
  (if (contains? #{:if-not-exists 'if-not-exists} (last spec))
    [(str "ALTER COLUMN " (sql/sql-kw :if-not-exists) " " (format-single-column (butlast spec)))]
    [(str "ALTER COLUMN " (format-single-column spec))]))


(sql/register-clause! :alter-column-raw format-alter-column :alter-column)


(defn- create-enum [_ x]
  [(str "CREATE TYPE " x " AS ENUM")])


(sql/register-clause! :create-enum create-enum :create-extension)


(defn- with-values [_ xs]
  [(str "("
        (str/join ", " (map #(str "'" % "'") xs))
        ")")])


(sql/register-clause! :with-values with-values :create-extension)


(defn- drop-enum [_ x]
  [(str "DROP TYPE " x)])


(sql/register-clause! :drop-enum drop-enum :create-extension)


(defn constraint [k xs]
  [(str (sql/sql-kw k) " " (format-single-column xs))])


(sql/register-clause! :add-constraint constraint :add-index)
(sql/register-clause! :drop-constraint constraint :drop-index)
