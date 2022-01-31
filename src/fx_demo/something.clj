(ns fx-demo.something)


(defn ^{:fx.module/autowire true}
  health-check [ctx request]
  {:status :ok})


(defn ^{:fx.module/autowire true}
  db-connection [request]
  {:connected :ok})


(defn ^{:fx.module/autowire :http-server/handler}
  status [^:fx-demo.something/db-connection db-connection request]
  {:status :ok})
