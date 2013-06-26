(ns aws-clj-sdk.s3.core
  (:use clojure.java.io
        roxxi.utils.print
        roxxi.utils.collections)
  (:import [com.amazonaws.services.s3.transfer
            TransferManager
            Transfer
            Transfer$TransferState])
  ;; for interactive development
  (:require [aws-clj-sdk.auth.core :as auth]))

;; stub