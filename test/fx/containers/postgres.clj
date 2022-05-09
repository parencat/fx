(ns fx.containers.postgres
  (:require
   [clj-test-containers.core :as tc]
   [integrant.core :as ig]
   [duct.core :as duct])
  (:import
   [org.testcontainers.containers PostgreSQLContainer]))


(defn pg-container [{:keys [port]
                     :or   {port 5432}}]
  (-> (tc/init {:container     (PostgreSQLContainer. "postgres:14.2")
                :exposed-ports [port]})
      (tc/start!)))


(defn stop [container]
  (tc/stop! container))


(defmethod ig/init-key :fx.containers/postgres [_ {:keys [port container]}]
  (if (some? container)
    container
    (pg-container {:port port})))


(defmethod ig/halt-key! :fx.containers/postgres [_ container]
  (stop container))


;; test helper module
(defmethod ig/init-key :fx.containers.postgres/connection [_ _]
  (let [container (pg-container {})]
    (fn [config]
      (duct/merge-configs
       config
       {:fx.database/connection {:url      (.getJdbcUrl (:container container))
                                 :user     (.getUsername (:container container))
                                 :password (.getPassword (:container container))}
        :fx.containers/postgres {:container container}}))))


(comment

 (def ^PostgreSQLContainer pgc
   (pg-container {:port 5432}))

 (.getJdbcUrl (:container pgc))
 (.getUsername (:container pgc))
 (.getPassword (:container pgc))

 (stop pgc)

 nil)
