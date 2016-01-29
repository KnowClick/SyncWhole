(ns kc.syncwhole.app
  (:require [taoensso.timbre :as log]
            [kc.syncwhole.core :refer [move!]])
  (:gen-class))

(defn -main [& args]
  (let [mapping-file (first args)]
    (if (nil? mapping-file)
      (do
        (log/error "Provide a mapping file.")
        (System/exit 1))
      (let [conf (read-string (slurp mapping-file))]
        (if (nil? conf)
          (do
            (log/error "Mapping file is empty.")
            (System/exit 2))
          (do (move! conf)
              (System/exit 0)))))))
