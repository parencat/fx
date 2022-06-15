(ns fx.module.autowire
  (:require
   [integrant.core :as ig]
   [clojure.java.classpath :as cp]
   [clojure.string]
   [clojure.tools.namespace.find :as tools.find]
   [malli.core :as m]))


(def ^:const AUTOWIRED-KEY :fx/autowire)
(def ^:const HALT-KEY :fx/halt)
(def ^:const WRAP-KEY :fx/wrap)


(defn find-project-namespaces
  "Will return all namespaces names as symbols from the classpath.
   Limit the number of resources by providing a pattern argument.
   By default, namespaces will be limited to user.dir folder.
   Passing nil as argument will lead to returning all namespaces."
  ([]
   (let [full-project-path (System/getProperty "user.dir")
         pattern           (re-find #"[^\/]+$" full-project-path)]
     (find-project-namespaces pattern)))

  ([pattern]
   (->> (cp/classpath)
        tools.find/find-namespaces
        (filter #(clojure.string/starts-with? % (str pattern))))))

(def namespace-pattern-s
  [:maybe [:or :string :symbol]])

(def project-namespaces
  [:sequential :symbol])

(m/=> find-project-namespaces
  [:function
   [:=> [:cat] project-namespaces]
   [:=> [:cat namespace-pattern-s] project-namespaces]])



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
  (let [arglists (:arglists component-meta)]
    (if (and (seq arglists)
             (> (count arglists) 1))
      (throw (ex-info "Multi-arity functions not supported by autowire module"
                      {:actual-arglists-count   (count arglists)
                       :expected-arglists-count 1}))
      (some->> arglists
               first
               (into [] meta->namespaced-keywords)))))


(defn prep-components-config [components]
  (reduce-kv (fn [config comp-key comp-value]
               (let [comp-meta     (meta comp-value)
                     parent        (get comp-meta AUTOWIRED-KEY)
                     halt-key      (get comp-meta HALT-KEY)
                     wrap?         (get comp-meta WRAP-KEY)
                     params-keys   (get-comp-deps comp-meta)
                     params-config (reduce (fn [acc param]
                                             (assoc acc (keyword (name param)) (ig/ref param)))
                                           {}
                                           params-keys)]

                 ;; inject component in config
                 (if (keyword? parent)
                   (assoc config [parent comp-key] (deref comp-value))

                   (do
                     (if (fn? (deref comp-value))
                       ;; component is function
                       (defmethod ig/init-key comp-key [_ params]
                         (let [params-values (mapv #(get params (keyword (name %))) params-keys)]
                           (if wrap?
                             (fn [& args]
                               (apply comp-value (concat params-values args)))

                             (apply comp-value params-values))))

                       ;; component is a constant value
                       (defmethod ig/init-key comp-key [_ _]
                         (deref comp-value)))

                     ;; add halt method if needed
                     (when (and (some? halt-key) (fn? halt-key))
                       (defmethod ig/halt-key! comp-key [_ init-result]
                         (halt-key init-result)))

                     (assoc config comp-key params-config)))))
             {}
             components))


(defmethod ig/init-key :fx.module/autowire [_ {:keys [root]}]
  (let [pns               (find-project-namespaces root)
        components        (find-components pns)
        components-config (prep-components-config components)]
    (fn [config]
      (merge config components-config))))
