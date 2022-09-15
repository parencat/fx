(ns fx.database
  (:require
   [integrant.core :as ig]
   [hikari-cp.core :as hikari-cp]
   [next.jdbc.connection :as jdbc.conn])
  (:import
   [javax.sql DataSource]))


(defn get-db-spec [{:keys [url user password dbtype host dbname port] :as params}]
  (let [jdbc-url (or url
                     (System/getenv "DATABASE_URL")
                     (try (jdbc.conn/jdbc-url
                           {:dbtype   dbtype
                            :dbname   dbname
                            :host     host
                            :port     port
                            :user     user
                            :password password})
                          (catch Exception error)))]
    (if (some? jdbc-url)
      (-> params
          (dissoc :url :dbtype :dbname :host :port :user :password)
          (assoc :jdbc-url jdbc-url))
      (throw (ex-info "Please provide a valid database jdbc url or db spec map.
                       See https://cljdoc.org/d/com.github.seancorfield/next.jdbc/1.2.780/api/next.jdbc#get-datasource"
                      {:component :fx.database/connection})))))



(defmethod ig/init-key :fx.database/connection [_ config]
  (let [spec (get-db-spec config)]
    (hikari-cp/make-datasource spec)))


(defmethod ig/halt-key! :fx.database/connection [_ ^DataSource ds]
  (hikari-cp/close-datasource ds))
