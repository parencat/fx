(ns fx.containers.postgres
  (:require
   [clj-test-containers.core :as tc]
   [integrant.core :as ig])
  (:import
   [org.testcontainers.containers PostgreSQLContainer]))


(defn pg-container [{:keys [port]
                     :or   {port 5432}}]
  (-> (tc/init {:container     (PostgreSQLContainer. "postgres:14.2")
                :exposed-ports [port]})
      (tc/start!)))


(defn stop [container]
  (tc/stop! container))


(defmethod ig/init-key :containers/postgres [_ {:keys [port]}]
  (pg-container {:port port}))


(defmethod ig/halt-key! :containers/postgres [container]
  (stop container))


(comment

 (def ^PostgreSQLContainer pgc
   (pg-container {:port 5432}))

 (.getJdbcUrl (:container pgc))
 (.getUsername (:container pgc))
 (.getPassword (:container pgc))

 (stop pgc)

 nil)
