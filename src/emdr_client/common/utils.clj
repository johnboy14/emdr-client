(ns emdr-client.common.utils
  (:require [clojure.core.async :refer [close!]]))

(defn close-chan [chan]
  (when chan
    (close! chan)))