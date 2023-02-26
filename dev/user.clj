(ns user
  (:require
   [clojure.pprint]
   [eftest.runner :as eftest]))


(def printer
  (bound-fn* clojure.pprint/pprint))

(add-tap printer)


(defn test []
  (remove-tap printer)

  (eftest/run-tests
   (eftest/find-tests "test")
   {:fail-fast?      true
    :capture-output? true
    :multithread?    false})

  (add-tap printer))


(comment
 (test)
 nil)
