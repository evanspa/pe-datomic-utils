(defproject pe-datomic-utils "0.0.12-SNAPSHOT"
  :description "A Clojure library of helper functions when working with Datomic."
  :url "https://github.com/evanspa/pe-datomic-utils"
  :license {:name "MIT"
            :url "http://opensource.org/licenses/MIT"}
  :plugins [[lein-pprint "1.1.2"]
            [codox "0.8.10"]]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/data.codec "0.1.0"]
                 [ch.qos.logback/logback-classic "1.0.13"]
                 [org.slf4j/slf4j-api "1.7.5"]
                 [pe-core-utils "0.0.7"]
                 [clj-time "0.8.0"]
                 [com.datomic/datomic-free "0.9.5130" ; need 'pro' to test log-related functions
                  :exclusions [org.slf4j/slf4j-nop
                               joda-time
                               org.slf4j/slf4j-log4j12]]]
  :resource-paths ["resources"]
  :codox {:exclude [user]
          :src-dir-uri "https://github.com/evanspa/pe-datomic-utils/blob/0.0.11/"
          :src-linenum-anchor-prefix "L"}
  :profiles {:dev {:source-paths ["dev"]  ;ensures 'user.clj' gets auto-loaded
                   :plugins [[cider/cider-nrepl "0.9.0-SNAPSHOT"]]
                   :dependencies [[org.clojure/tools.namespace "0.2.7"]
                                  [org.clojure/java.classpath "0.2.2"]
                                  [org.clojure/data.json "0.2.5"]
                                  [org.clojure/tools.nrepl "0.2.7"]
                                  [org.postgresql/postgresql "9.3-1102-jdbc41"]]
                   :resource-paths ["test-resources"]}
             :test {:resource-paths ["test-resources"]}}
  :repositories [["releases" {:url "https://clojars.org/repo"
                              :creds :gpg}]])
