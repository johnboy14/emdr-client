(ns emdr-client.switchboard.switchboard
  (:require [com.stuartsierra.component :as comp]
            [clojure.core.async :refer [pub sub mult tap]]
            [clojure.tools.logging :as log]
            [emdr-client.common.utils :refer [close-chan]]))

(defrecord Switchboard [mkdata-chan rethink-chans]
  comp/Lifecycle
  (start [comp]
    (log/info "Starting Switchboard")
    (let [mkdata-mult (mult (:chan mkdata-chan))]
      (tap mkdata-mult (:order-chan rethink-chans))
      (tap mkdata-mult (:history-chan rethink-chans))
      (log/info "Started Switchboard")
      (assoc comp :mkdata-mult mkdata-mult)))
  (stop [comp]
    (log/info "Shutting down Switchboard")
    (->
      (update-in comp [:mkdata-chan :chan] close-chan)
      (update-in [:rethink-chans :order-chan] close-chan)
      (update-in [:rethink-chans :history-chan] close-chan))))

(defn new-switchboard []
  (map->Switchboard {}))
