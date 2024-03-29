(ns fx.containers.postgres
  (:require
   [clj-test-containers.core :as tc]
   [integrant.core :as ig]
   [duct.core :as duct]
   [next.jdbc :as jdbc])
  (:import
   [org.testcontainers.containers PostgreSQLContainer]))


(defn pg-container
  ([]
   (pg-container {}))

  ([{:keys [port add-resources]
     :or   {port          5432
            add-resources false}}]
   (cond-> (tc/init {:container     (PostgreSQLContainer. "postgres:14.2")
                     :exposed-ports [port]})
           add-resources
           (tc/bind-filesystem! {:host-path      "resources"
                                 :container-path "/resources"
                                 :mode           :read-only})
           :always
           (tc/start!))))


(defn stop [container]
  (tc/stop! container))


(defmethod ig/init-key :fx.containers/postgres [_ {:keys [port container]
                                                   :or   {port 5432}}]
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
       {:fx.database/connection {:url      (format "%s&user=%s&password=%s"
                                                   (.getJdbcUrl (:container container))
                                                   (.getUsername (:container container))
                                                   (.getPassword (:container container)))
                                 :postgres (ig/ref :fx.containers/postgres)}
        :fx.containers/postgres {:container container}}))))


;; test helper macro
(defmacro with-datasource [symbol & body]
  `(let [container# (pg-container {:port 5432})
         port#      (get (:mapped-ports container#) 5432)
         host#      (:host container#)
         user#      (.getUsername (:container container#))
         password#  (.getPassword (:container container#))
         url#       (str "jdbc:postgresql://" host# ":" port# "/test?user=" user# "&password=" password#)
         ~symbol (jdbc/get-datasource {:jdbcUrl url#})]
     (let [result# (try
                     (do ~@body)
                     (catch Exception ex#
                       ex#))]
       (stop container#)
       (if (instance? Exception result#)
         (throw result#)
         result#))))



(comment

 (def ^PostgreSQLContainer pgc
   (pg-container {:port 5432}))

 (.getJdbcUrl (:container pgc))
 (.getUsername (:container pgc))
 (.getPassword (:container pgc))

 (stop pgc)


 (def ds
   (jdbc/get-datasource
    {:jdbcUrl (format "%s&user=%s&password=%s"
                      (.getJdbcUrl (:container pgc))
                      (.getUsername (:container pgc))
                      (.getPassword (:container pgc)))}))

 (jdbc/execute! ds [""]
   {:return-keys true})

 nil)
