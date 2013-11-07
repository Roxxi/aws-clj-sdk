(defproject roxxi/aws-clj-sdk "0.1.1"
  :description "Clojure Wrapper around Amazon AWS Java SDK"
  :url "http://github.com/Roxxi/aws-clj-sdk"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.amazonaws/aws-java-sdk "1.4.7"]
                 [roxxi/clojure-common-utils "0.0.14"]
                 ;; Should move this to development dependencies later
                 [clj-yaml "0.4.0"]]
  :resource-paths ["conf"]
  :warn-on-reflection true)
