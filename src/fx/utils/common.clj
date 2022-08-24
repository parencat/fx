(ns fx.utils.common
  (:require
   [malli.core :as m]))


(defn entity-key->entity-type [entity-key]
  (if (vector? entity-key)
    (second entity-key)
    entity-key))

(m/=> entity-key->entity-type
  [:=> [:cat [:or :qualified-keyword [:vector :qualified-keyword]]]
   :qualified-keyword])
