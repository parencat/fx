(ns fx.module.autowire
  (:require [integrant.core :as ig]
            [clojure.java.classpath :as cp]
            [clojure.string]
            [clojure.tools.namespace.find :as tools.find]))


(def ^:const AUTOWIRED-KEY :fx/autowire)
(def ^:const HALT-KEY :fx/halt)
(def ^:const WRAP-KEY :fx/wrap)


(defn find-project-namespaces [root-ns]
  (->> (cp/classpath)
       tools.find/find-namespaces
       (filter #(clojure.string/starts-with? % (str root-ns)))))


(defn collect-autowired [ns autowired item-key item-val]
  (let [item-meta (meta item-val)]
    (if (some? (get item-meta AUTOWIRED-KEY))
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


(def meta->namespaced-keywords
  (comp (map meta)
        (remove nil?)
        (mapcat keys)
        (filter namespace)))


(defn get-comp-deps [component-meta]
  (let [arslists (:arglists component-meta)]
    (if (and (seq arslists)
             (> (count arslists) 1))
      (throw (ex-info "Multi-arity functions not supported by autowire module"
                      {:actual-arslists-count   (count arslists)
                       :expected-arslists-count 1}))
      (some->> arslists
               first
               (into [] meta->namespaced-keywords)))))


(defn prep-components-config [components]
  (reduce-kv (fn [config comp-key comp-value]
               (let [comp-meta     (meta comp-value)
                     halt-key      (get comp-meta HALT-KEY)
                     wrap?         (get comp-meta WRAP-KEY)
                     params-keys   (get-comp-deps comp-meta)
                     params-config (reduce (fn [acc param]
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

                 (assoc config comp-key params-config)))
             {}
             components))


(defmethod ig/init-key :fx.module/autowire [_ {:keys [root]}]
  (let [pns               (find-project-namespaces root)
        components        (find-components pns)
        components-config (prep-components-config components)]
    (fn [config]
      (merge config components-config))))
