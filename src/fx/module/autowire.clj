(ns fx.module.autowire
  (:require [integrant.core :as ig]
            [clojure.java.classpath :as cp]
            [clojure.string]
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
                     halt-key         (:fx.module/halt-key comp-meta)
                     wrap?            (:fx.module/wrap-fn comp-meta)
                     comp-key'        (if (keyword? parent-component)
                                        [parent-component comp-key]
                                        comp-key)
                     params-keys      (some->> comp-value
                                               meta
                                               :arglists
                                               first
                                               (into [] (comp (map meta)
                                                              (remove nil?)
                                                              (mapcat keys)
                                                              (filter namespace))))
                     params-config    (reduce (fn [acc param]
                                                (assoc acc (keyword (name param)) (ig/ref param)))
                                              {}
                                              params-keys)]

                 (if (fn? (deref comp-value))
                   (defmethod ig/init-key comp-key [_ params]
                     (let [params-values (mapv #(get params (keyword (name %))) params-keys)]
                       (if wrap?
                         (fn [& args]
                           (apply comp-value (concat params-values args)))

                         (apply comp-value params-values))))

                   (defmethod ig/init-key comp-key [_ _]
                     (deref comp-value)))

                 (when (and (some? halt-key) (fn? halt-key))
                   (defmethod ig/halt-key! comp-key [_ init-result]
                     (halt-key init-result)))

                 (assoc config comp-key' params-config)))
             {}
             components))


(defmethod ig/init-key :fx.module/autowire [_ {:keys [project-ns]}]
  (let [pns               (find-project-namespaces project-ns)
        components        (find-components pns)
        components-config (prep-components-config components)]
    (fn [config]
      (merge config components-config))))
