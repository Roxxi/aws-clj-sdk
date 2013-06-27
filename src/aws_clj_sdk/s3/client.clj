(ns aws-clj-sdk.s3.client
  (:use clojure.java.io
        roxxi.utils.print
        roxxi.utils.collections)
  (:import[com.amazonaws.auth AWSCredentials]
          [com.amazonaws.services.s3 AmazonS3])
  ;; for interactive development
  (:require [aws-clj-sdk.auth.core :as auth]))