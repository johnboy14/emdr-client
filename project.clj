(defproject emdr_client "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.stuartsierra/component "0.3.1"]
                 [org.zeromq/jeromq "0.3.4"]
                 [org.clojure/core.async "0.2.374"]
                 [org.zeromq/jeromq "0.3.4"]
                 [org.clojure/tools.logging "0.3.1"]
                 [cheshire "5.4.0"]
                 [com.apa512/rethinkdb "0.15.23"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.4"]]
                   :source-paths ["dev"]}})
