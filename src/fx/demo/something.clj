(ns fx.demo.something)


(def ^:fx/autowire constant-value
  {:connected :ok})


(defn ^:fx/autowire health-check []
  (fn [ctx req]
    {:status :ok}))


(defn ^{:fx/autowire true}
  db-connection []
  (fn []
    {:connected :ok}))


(defn status
  {:fx/autowire true}
  [^:fx.demo.something/db-connection db-connection]
  (fn []
    {:status     :ok
     :connection (db-connection)}))


(defn ^{:fx/autowire true}
  other-handler [^:fx.demo.something/db-connection db-connection]
  (fn []
    {:status     :fail
     :connection (db-connection)}))
