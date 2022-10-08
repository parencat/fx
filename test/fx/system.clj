(ns fx.system
  (:require
   [duct.core :as duct]
   [integrant.core :as ig]))


(def ^:dynamic *system* nil)


(defmacro with-system [[symbol config] & body]
  `(binding [*system* *system*]
     (try
       (let [conf# (duct/prep-config ~config)
             ~symbol (ig/init conf#)]
         (set! *system* ~symbol)
         (do ~@body)
         (ig/halt! ~symbol))
       (catch Throwable t#
         (println "Error in test:" (ex-cause t#))
         (when (some? *system*)
           (ig/halt! *system*))
         (throw t#))
       (finally
         (set! *system* nil)))))
