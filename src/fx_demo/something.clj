(ns fx-demo.something)


(defn ^:http-handler/health-check health-check [system request]
  {:status :ok})
