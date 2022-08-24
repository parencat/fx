(ns fx.database
  (:require
   [integrant.core :as ig]
   [medley.core :as mdl]
   [next.jdbc :as jdbc]
   [next.jdbc.connection :as jdbc.conn])
  (:import
   [java.sql Connection]))


(defmethod ig/prep-key :fx.database/connection [_ {:keys [url user password
                                                          dbtype host dbname port]}]
  (let [jdbc-url (or url
                     (System/getenv "DATABASE_URL")
                     (try (jdbc.conn/jdbc-url {:dbtype dbtype :dbname dbname :host host :port port})
                          (catch Exception error)))]
    (if (some? jdbc-url)
      (mdl/assoc-some
       {:jdbcUrl jdbc-url}
       :user user
       :password password)

      (throw (ex-info "Please provide a valid database jdbc url or db spec map.
                       See https://cljdoc.org/d/com.github.seancorfield/next.jdbc/1.2.780/api/next.jdbc#get-datasource"
                      {:component :fx.database/connection})))))


(defmethod ig/init-key :fx.database/connection [_ connection-params]
  ;; TODO use connection pool
  (jdbc/get-connection connection-params))


(defmethod ig/halt-key! :fx.database/connection [_ ^Connection connection]
  (.close connection))
