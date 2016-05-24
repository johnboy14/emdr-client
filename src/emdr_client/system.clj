(ns emdr-client.system
  (:require [com.stuartsierra.component :as component]
            [emdr-client.emdr.client :refer [new-channel new-emdr-client]]
            [emdr-client.persistence.rethink :refer [new-rethinkdb-channels]]
            [emdr-client.switchboard.switchboard :refer [new-switchboard]]))

(defn new-system []
  (component/system-map
    :emdr-mkdata-chan (new-channel 10)
    :rethinkdb-chans (new-rethinkdb-channels 10)
    :switchboard (component/using
                   (new-switchboard)
                   {:mkdata-chan :emdr-mkdata-chan
                    :rethink-chans :rethinkdb-chans})
    :emdr-client (component/using
                   (new-emdr-client "tcp://relay-us-central-1.eve-emdr.com:8050" 10)
                   {:mkdata-chan :emdr-mkdata-chan})))
