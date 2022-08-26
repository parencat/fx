(ns fx.module.autowire
  (:require
   [integrant.core :as ig]
   [clojure.java.classpath :as cp]
   [clojure.string]
   [clojure.tools.namespace.find :as tools.find]
   [malli.core :as m]
   [fx.utils.loader :as loader]))


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

(m/=> find-project-namespaces
  [:function
   [:=> [:cat]
    [:sequential :symbol]]
   [:=> [:cat [:maybe [:or :string :symbol]]]
    [:sequential :symbol]]])


(defn collect-autowired
  "Assoc key value pair in the autowired map in case if value meta contains AUTOWIRED-KEY"
  [ns autowired item-key item-val]
  (let [item-meta (meta item-val)]
    (if (some? (get item-meta AUTOWIRED-KEY))
      (assoc autowired (keyword ns (str item-key)) item-val)
      autowired)))

(m/=> collect-autowired
  [:=> [:cat :string :map :symbol :any]
   [:map-of :qualified-keyword :any]])


(defn find-components
  "Given a list of namespaces traverses through all public members.
   Returns a map containing all ns members who have AUTOWIRED-KEY meta"
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
  "Given a function metadata map will traverse all function arguments
   and collect all items which has a qualified keywords in the metadata.
   E.g. for function like (defn my-func [^:some/dependency dep] ...)
   will return list (:some/dependency)"
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
   [:sequential :qualified-keyword]])


(defn get-params-config [params-keys]
  (reduce (fn [acc param]
            (assoc acc (keyword (name param)) (ig/ref param)))
          {} params-keys))

(m/=> get-params-config
  [:=> [:cat [:sequential :qualified-keyword]]
   [:map-of :keyword ig-ref]])


(defn prep-component
  "Creates an integrant key method and assoc this key in the resulting config"
  [config comp-key comp-value]
  (let [comp-meta     (meta comp-value)
        halt-key      (get comp-meta HALT-KEY)
        wrap?         (get comp-meta WRAP-KEY)
        params-keys   (get-comp-deps comp-meta)
        params-config (get-params-config params-keys)
        fn-comp?      (fn? (deref comp-value))]

    (cond
      (and fn-comp? (keyword? halt-key))
      (defmethod ig/halt-key! halt-key [_ init-result]
        (comp-value init-result))

      fn-comp?
      (defmethod ig/init-key comp-key [_ params]
        (let [params-values (mapv #(get params (keyword (name %))) params-keys)]
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
  "Given a list of system components will create an integrant key for each of them.
   Returns integrant style config map for given components."
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
      (merge config components-config))))
