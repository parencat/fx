(ns fx.module.autowire
  (:require
   [integrant.core :as ig]
   [clojure.java.classpath :as cp]
   [clojure.string]
   [clojure.tools.namespace.find :as tools.find]
   [malli.core :as m]
   [fx.utils.loader :as loader]
   [duct.core :as duct]))


(def ig-ref
  (m/-simple-schema
   {:type :ig/ref
    :pred ig/ref?}))


(def ^:const AUTOWIRED-KEY :fx/autowire)
(def ^:const HALT-KEY :fx/halt)
(def ^:const WRAP-KEY :fx/wrap)


(defn find-project-namespaces
  "Will return all namespaces names as symbols from the classpath.
   Limit the number of resources by providing a pattern argument.
   By default, namespaces will be limited to `user.dir` path.
   Passing nil as argument will lead to returning all namespaces."
  ([]
   (let [project-path (System/getProperty "user.dir")
         pattern      (re-find #"[^\/]+$" project-path)]
     (find-project-namespaces pattern)))

  ([pattern]
   (->> (cp/classpath)
        tools.find/find-namespaces
        (filter #(clojure.string/starts-with? % (str pattern))))))

(m/=> find-project-namespaces
  [:function
   [:=> [:cat]
    [:sequential :symbol]]
   [:=> [:cat [:maybe [:or :string :symbol]]]
    [:sequential :symbol]]])


(defn collect-autowired
  "Assoc key value pair in the autowired map
   in case if the value metadata contains AUTOWIRED-KEY.
   Returns autowired map."
  [ns autowired item-key item-val]
  (let [item-meta (meta item-val)]
    (if (some? (get item-meta AUTOWIRED-KEY))
      (assoc autowired (keyword ns (str item-key)) item-val)
      autowired)))

(m/=> collect-autowired
  [:=> [:cat :string :map :symbol :any]
   [:map-of :qualified-keyword :any]])


(defn find-components
  "Given a list of namespaces will scan through all public members.
   Returns a map containing all ns members who contain AUTOWIRED-KEY in the metadata"
  [namespaces]
  (->> (for [ns namespaces]
         (let [_         (loader/require ns)
               members   (ns-publics ns)
               ns-string (str ns)]
           (reduce-kv (partial collect-autowired ns-string)
                      {} members)))
       (apply merge)))

(m/=> find-components
  [:=> [:cat [:sequential :symbol]]
   [:map-of :qualified-keyword :any]])


(def meta->namespaced-keywords
  (comp (map meta)
        (remove nil?)
        (mapcat keys)
        (filter namespace)))


(defn get-comp-deps
  "Given a function metadata map will traverse all function arguments (:arglists)
   and collect all items which has a qualified keywords in the metadata.
   E.g. for function like (defn my-func [^:some/dependency dep] ...)
   will return a vector like [:some/dependency].
   Doesn't support multi-arity functions atm."
  [component-meta]
  (let [arglists (:arglists component-meta)]
    (if (and (seq arglists)
             (> (count arglists) 1))
      (throw (ex-info "Multi-arity functions not supported by autowire module"
                      {:actual-arglists-count   (count arglists)
                       :expected-arglists-count 1}))
      (->> arglists
           first
           (into [] meta->namespaced-keywords)))))

(m/=> get-comp-deps
  [:=> [:cat [:map [:arglists {:optional true} seq?]]]
   [:vector :qualified-keyword]])


(defn get-params-config
  "Given a vector of function parameters names returns a configuration map
   with integrant references.
   E.g. [:some/dependency] => {:some/dependency (ig/ref :some/dependency)}"
  [deps-keys]
  (reduce (fn [acc param]
            (assoc acc param (ig/ref param)))
          {} deps-keys))

(m/=> get-params-config
  [:=> [:cat [:vector :qualified-keyword]]
   [:map-of :qualified-keyword ig-ref]])


(defn prep-component
  "Prepare a single component. Few things required for every component:
   1. initialize integrant methods for that component (ig/init-key, ig/halt-key!)
   2. find component dependencies and create integrant references for them
   3. add a component configuration to the main config map"
  [config comp-key comp-value]
  (let [comp-meta     (meta comp-value)
        halt-key      (get comp-meta HALT-KEY)
        wrap?         (get comp-meta WRAP-KEY)
        deps-keys     (get-comp-deps comp-meta)
        params-config (get-params-config deps-keys)
        fn-comp?      (fn? (deref comp-value))]
    (cond
      (and fn-comp? (keyword? halt-key))
      (defmethod ig/halt-key! halt-key [_ init-result]
        (comp-value init-result))

      fn-comp?
      (defmethod ig/init-key comp-key [_ params]
        (let [deps-values   (if (seq deps-keys)
                              ((apply juxt deps-keys) params)
                              [])
              rest-params   (apply dissoc params deps-keys)
              params-values (if (not-empty rest-params)
                              (conj deps-values rest-params)
                              deps-values)]
          (if wrap?
            ;; return component as anonymous function
            (fn [& args]
              (apply comp-value (concat params-values args)))
            ;; or call it immediately
            (apply comp-value params-values))))

      :else
      (defmethod ig/init-key comp-key [_ _]
        (deref comp-value)))

    (if (keyword? halt-key)
      config
      (assoc config comp-key params-config))))

(def composite-integrant-key
  [:vector {:min 2 :max 2} :qualified-keyword])

(m/=> prep-component
  [:=> [:cat :map :qualified-keyword :any]
   [:map-of
    [:or :qualified-keyword composite-integrant-key]
    [:or [:map-of :keyword ig-ref] :any]]])


(defn prep-components-config
  "Given a map of components will prepare integrant lifecycle methods
   and configuration for each of them.
   Returns an integrant style config map for all given components."
  [components]
  (reduce-kv
   (fn [config comp-key comp-value]
     (let [comp-meta (meta comp-value)
           parent    (get comp-meta AUTOWIRED-KEY)]
       ;; child components doesn't require additional processing
       (if (keyword? parent)
         (assoc config [parent comp-key] (deref comp-value))
         (prep-component config comp-key comp-value))))
   {} components))

(m/=> prep-components-config
  [:=> [:cat [:map-of :qualified-keyword :any]]
   [:map-of
    [:or :qualified-keyword composite-integrant-key]
    [:or [:map-of :keyword ig-ref] :any]]])


;; =============================================================================
;; Duct integration
;; =============================================================================

(defmethod ig/init-key :fx.module/autowire [_ {:keys [root]}]
  (let [pns               (find-project-namespaces root)
        components        (find-components pns)
        components-config (prep-components-config components)]
    (fn [config]
      (duct/merge-configs config components-config))))
