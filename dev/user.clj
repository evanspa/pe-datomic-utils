(ns user
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer (pprint)]
            [clojure.repl :refer :all]
            [clojure.stacktrace :refer (e)]
            [clj-time.core :as t]
            [clojure.test :as test]
            [datomic.api :as d]
            [clojure.java.io :refer [resource]]
            [clojure.tools.namespace.repl :refer (refresh refresh-all)]))
