(ns modules.autowire
  (:require [integrant.core :as ig]
            [clojure.java.classpath :as cp]
            [clojure.tools.namespace.find :as tools.find]))


(defn find-project-namespaces [ns]
  (->> (cp/classpath)
       tools.find/find-namespaces
       (filter #(clojure.string/starts-with? % (str ns)))))


(defn collect-autowired [ns autowired item-key item-val]
  (let [item-meta (meta item-val)]
    (if (some? (:fx.module/autowire item-meta))
      (assoc autowired (keyword ns (str item-key)) item-val)
      autowired)))


(defn find-components [namespaces]
  (->> (for [ns namespaces]
         (let [_         (require ns :reload)
               members   (ns-publics ns)
               ns-string (str ns)]
           (reduce-kv (partial collect-autowired ns-string)
                      {}
                      members)))
       (apply merge)))


(defn prep-components-config [components]
  (reduce-kv (fn [config comp-key comp-value]
               (let [comp-meta        (meta comp-value)
                     parent-component (:fx.module/autowire comp-meta)
                     comp-key'        (if (keyword? parent-component)
                                        [parent-component comp-key]
                                        comp-key)]
                 (defmethod ig/init-key comp-key [_ _] comp-value)
                 (assoc config comp-key' {})))
             {}
             components))


(defmethod ig/init-key :fx.module/autowire [_ {:keys [project-ns]}]
  (let [pns               (find-project-namespaces project-ns)
        components        (find-components pns)
        components-config (prep-components-config components)]
    (fn [config]
      (merge config components-config))))




(comment
 (->> (tools.find/find-namespaces (cp/classpath))
      (filter (fn [ns] (-> ns (clojure.string/starts-with? "fx-demo")))))

 (find-project-namespaces 'fx-demo)

 (->> (find-project-namespaces 'fx-demo)
      (find-components)
      (prep-components-config))

 (require (symbol 'fx-demo.something) :reload)
 (meta ('status (ns-publics (symbol 'fx-demo.something))))
 (ns-resolve 'fx-demo.something 'health-check))
