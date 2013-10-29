(ns aws-clj-sdk.s3.core-test
  (:require [clj-yaml.core :as yaml]
            [aws-clj-sdk.s3.core :as s3]
            [aws-clj-sdk.s3.client :as s3c]
            [aws-clj-sdk.auth.core-test :as auth]))

(defn s3client []
  (s3c/make-s3-client 
   (auth/make-creds-from-file auth/credential-file)))

