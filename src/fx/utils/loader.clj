(ns fx.utils.loader
  (:refer-clojure :exclude [require])
  (:require
   [clojure.string :as str]
   [malli.core :as m])
  (:import
   [clojure.lang DynamicClassLoader RT]))


(def classloader?
  (m/-simple-schema
   {:type :classloader
    :pred #(instance? ClassLoader %)}))


(defonce ^:private shared-context-classloader
  (delay
   (or
    (when-let [base-loader (RT/baseLoader)]
      (when (instance? DynamicClassLoader base-loader)
        base-loader))
    (let [new-classloader (DynamicClassLoader. (.getContextClassLoader (Thread/currentThread)))]
      new-classloader))))


(defn- has-classloader-as-ancestor? [^ClassLoader classloader ^ClassLoader ancestor]
  (cond
    (identical? classloader ancestor)
    true

    classloader
    (recur (.getParent classloader) ancestor)

    :else
    false))

(m/=> has-classloader-as-ancestor?
  [:=> [:cat classloader? classloader?]
   :boolean])


(defn- has-shared-context-classloader-as-ancestor? [^ClassLoader classloader]
  (has-classloader-as-ancestor? classloader @shared-context-classloader))

(m/=> has-shared-context-classloader-as-ancestor?
  [:=> [:cat classloader?]
   :boolean])


(defn the-classloader ^ClassLoader []
  (or
   (let [current-thread-context-classloader (.getContextClassLoader (Thread/currentThread))]
     (when (has-shared-context-classloader-as-ancestor? current-thread-context-classloader)
       current-thread-context-classloader))
   (let [shared-classloader @shared-context-classloader]
     (.setContextClassLoader (Thread/currentThread) shared-classloader)
     shared-classloader)))

(m/=> the-classloader
  [:=> :cat
   classloader?])


(defn- require* [& args]
  (when-not *compile-files*
    (the-classloader)
    (try
      (binding [*use-context-classloader* true]
        (locking RT/REQUIRE_LOCK
          (apply clojure.core/require args)))
      (catch Throwable e
        ;; TODO might be better to just log such issues rather throwing exceptions
        (throw (ex-info (.getMessage e)
                        {:classloader      (the-classloader)
                         :system-classpath (sort (str/split (System/getProperty "java.class.path") #"[:;]"))}
                        e))))))

(m/=> require*
  [:=> [:cat [:* :symbol]]
   :nil])


(defn require
  ([x]
   (let [libs            (loaded-libs)
         already-loaded? (and (symbol? x)
                              (contains? libs x))]
     (when-not already-loaded?
       (require* x))))

  ([x & more]
   (apply require* x more)))

(m/=> require
  [:function
   [:=> [:cat :symbol]
    :nil]
   [:=> [:cat [:* :symbol]]
    :nil]])
