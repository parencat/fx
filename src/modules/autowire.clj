(ns modules.autowire
  (:require [integrant.core :as ig]
            [clojure.java.classpath :as cp]
            [clojure.tools.namespace.find :as tools.find]))


(defn find-project-namespaces [ns]
  (->> (cp/classpath)
       tools.find/find-namespaces
       (filter #(clojure.string/starts-with? % ns))))


(defn find-components [namespaces])


(defn prep-components-config [components])


(defmethod ig/init-key :fx.module/autowire [_ {:keys [project-ns]}]
  (let [pns               (find-project-namespaces project-ns)
        components        (find-components pns)
        components-config (prep-components-config components)]
    (fn [config]
      (merge config components-config))))




(comment
 (->> (tools.find/find-namespaces (cp/classpath))
      (filter (fn [ns] (-> ns (clojure.string/starts-with? "fx-demo")))))

 (require (symbol 'fx-demo.something))
 (ns-publics (symbol 'fx-demo.something))
 (ns-resolve 'fx-demo.something 'health-check))
