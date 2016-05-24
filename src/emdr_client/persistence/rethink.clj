(ns emdr-client.persistence.rethink
  (:require [com.stuartsierra.component :as comp]
            [clojure.tools.logging :as log]))

(defrecord RethinkDBChannels [buffer-size]
  comp/Lifecycle
  (start [comp]
    comp)
  (stop [comp]
    comp))

(defn new-rethinkdb-channels [buffer-size]
  (map->RethinkDBChannels {:buffer-size buffer-size}))