(ns fx.database
  (:require
   [integrant.core :as ig]
   [next.jdbc :as jdbc]))


(defmethod ig/init-key :fx/database [_ {:keys [db-uri db-type]}]
  (jdbc/get-connection {:connection-uri db-uri
                        :dbtype         db-type}))
