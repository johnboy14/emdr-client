(ns emdr-client.system
  (:require [com.stuartsierra.component :as component]
            [emdr-client.emdr.client :refer [new-channel new-emdr-client]]
            [emdr-client.persistence.rethink :refer [new-rethinkdb-channels
                                                     new-rethinkDB-writers]]
            [emdr-client.switchboard.switchboard :refer [new-switchboard]]))

(defn new-system []
  (component/system-map
    :emdr-mkdata-chan (new-channel 100)
    :rethinkdb-chans (new-rethinkdb-channels 100)
    :switchboard (component/using
                   (new-switchboard)
                   {:mkdata-chan :emdr-mkdata-chan
                    :rethink-chans :rethinkdb-chans})
    :emdr-client (component/using
                   (new-emdr-client "tcp://relay-us-central-1.eve-emdr.com:8050" 2)
                   {:mkdata-chan :emdr-mkdata-chan})
    :rethinkdb-consumer (component/using
                          (new-rethinkDB-writers "127.0.0.1" 28015 2)
                          {:rethink-chans :rethinkdb-chans})))
