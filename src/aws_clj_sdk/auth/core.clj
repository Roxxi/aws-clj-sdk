(ns aws-clj-sdk.auth.core
  (:use roxxi.utils.print
        roxxi.utils.collections)
  (:import [com.amazonaws.auth BasicAWSCredentials]))

(defn make-credentials [access-key secret-key]
  (BasicAWSCredentials. access-key secret-key))
