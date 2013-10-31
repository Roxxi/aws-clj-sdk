(ns aws-clj-sdk.auth.core-test
  (:require [clj-yaml.core :as yaml]
            [aws-clj-sdk.auth.core :as auth]))

(def credential-file "conf/aws.yml")

(defn make-creds-from-file [credential-file]
  (let [config (yaml/parse-string (slurp credential-file))]
    (auth/make-credentials (:access-key (:aws config))
                           (:secret-key (:aws config)))))
