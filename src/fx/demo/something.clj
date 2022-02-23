(ns fx.demo.something)


(defn ^:fx.module/autowire health-check [ctx req]
  {:status :ok})


(defn ^{:fx.module/autowire true}
  db-connection []
  {:connected :ok})


(def ^:fx.module/autowire constant-value
  {:connected :ok})


(defn ^{:fx.module/autowire :http-server/handler}
  status [^:fx.demo.something/db-connection db-connection]
  {:status     :ok
   :connection (db-connection)})


(defn ^{:fx.module/autowire :http-server/handler}
  other-handler [^:fx.demo.something/db-connection db-connection]
  {:status     :fail
   :connection (db-connection)})
