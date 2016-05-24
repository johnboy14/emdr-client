(ns emdr-client.system
  (:require [com.stuartsierra.component :as component]
            [emdr-client.emdr.client :refer [new-channel new-emdr-client]]))

(defn new-system []
  (component/system-map
    :emdr-mkdata-chan (new-channel 10)
    :emdr-client (component/using
                   (new-emdr-client "tcp://relay-us-central-1.eve-emdr.com:8050" 10)
                   {:mkdata-chan :emdr-mkdata-chan})))
