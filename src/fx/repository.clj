(ns fx.repository)


(defprotocol PRepository
  (create! [_ params])
  (update! [_])
  (find! [_])
  (find-all! [_])
  (delete! [_]))
