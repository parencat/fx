(ns fx.demo.something)


(def ^:fx.module/autowire constant-value
  {:connected :ok})


(defn ^:fx.module/autowire health-check []
  (fn [ctx req]
    {:status :ok}))


(defn ^{:fx.module/autowire true}
  db-connection []
  (fn []
    {:connected :ok}))


(defn status
  {:fx.module/autowire :http-server/handler}
  [^:fx.demo.something/db-connection db-connection]
  (fn []
    {:status     :ok
     :connection (db-connection)}))


(defn ^{:fx.module/autowire :http-server/handler}
  other-handler [^:fx.demo.something/db-connection db-connection]
  (fn []
    {:status     :fail
     :connection (db-connection)}))
