(ns fx.repository)


(defprotocol PRepository
  (create! [_])
  (update! [_])
  (find! [_])
  (find-all! [_])
  (delete! [_]))
