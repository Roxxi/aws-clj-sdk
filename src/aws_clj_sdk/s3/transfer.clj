(ns aws-clj-sdk.s3.transfer
  (:require [clojure.java.io :refer [file]])
  (:require [roxxi.utils.collections :refer [seq->java-list]])
  (:use roxxi.utils.common)
  (:import [java.io ByteArrayInputStream]
           [com.amazonaws.auth AWSCredentials]
           [com.amazonaws.services.s3 AmazonS3]
           [com.amazonaws.services.s3.model
            ObjectMetadata
            ProgressListener
            ProgressEvent]
           [com.amazonaws.services.s3.transfer
            TransferManager
            Transfer
            Transfer$TransferState]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Protocols
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol S3Downloader
  (download! [_ bucket key file]
    "Schedules a new transfer to download data from Amazon S3
and save it to the specified file.")
  (download-directory! [_ bucket-name key-prefix dest-dir]
    "Downloads all objects in the virtual directory
designated by the keyPrefix given to the destination directory given."))

(defprotocol S3Uploader
  (upload! [_ bucket key file]
    "Uploads all files in the directory given to the bucket named,
optionally recursing for all subdirectories.")
  (upload-string-as-file! [_ bucket key string]
    "Uploads the string to the bucket at the specified key.")
  (upload-directory! [_ bucket dir-key-prefix local-dir recursive?]
    "Schedules a new transfer to upload data to Amazon S3.")
  (upload-files! [_ bucket dir-key-prefix local-dir files]
    "Uploads all specified files to the bucket named,
constructing relative keys depending on the commonParentDirectory given."))

(defprotocol S3TransferManager
  (shutdown-now! [_] "Forcefully shuts down this TransferManager instance -
currently executing transfers will not be allowed to finish.
If an application is long running, this will also release all download /
upload threads that have been allocated, and should be called to prevent
thread leakage"))


(defprotocol S3Transfer
  (done? [_]
    "Returns whether or not the transfer is finished
 (i.e. completed successfully, failed, or was canceled).")
  (state [_]
    "Returns the current state of this transfer.")
  (description [_]
    "Returns a human-readable description of this transfer.")
  (wait-for-completion [_]
    "Waits for this transfer to complete. This is a blocking call;
the current thread is suspended until this transfer completes.")
  (wait-for-exception [_]
    "Waits for this transfer to finish and returns any error that occurred,
or returns null if no errors occurred. This is a blocking call;
the current thread will be suspended until this transfer either
fails or completes successfully.")
  ;; Omitted the Progress Listeners for now...
  ;; Wrap around TransferProgress
  (bytes-transfered [_]
    "Returns the number of bytes completed in the associated transfer.")
  (percent-transfered [_]
    "Returns a percentage of the number of bytes transfered out
of the total number of bytes to transfer.")
  (total-bytes-to-transfer [_]
    "Returns the total size in bytes of the associated transfer,
or -1 if the total size isn't known.")
  (add-progress-listener [t listener]
    "Adds the specified progress listener to the list of listeners
receiving updates about this transfer's progress.
Returns a reference to this transfer"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Wrapping of `com.amazonaws.services.s3.transfer`
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## Helpers
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn fileify [file-or-path]
  (if (string? file-or-path)
    (file file-or-path)
    file-or-path))

(defn- transfer-progress [^Transfer t]
  (.getProgress t))

(def- state=>keywd
  {Transfer$TransferState/Canceled :canceled
   Transfer$TransferState/Completed :completed
   Transfer$TransferState/Failed :failed
   Transfer$TransferState/InProgress :in-progress
   Transfer$TransferState/Waiting :waiting})

;; n.b. see http://grammarist.com/spelling/cancel/
(def- keywd=>state
  {:cancelled Transfer$TransferState/Canceled
   :canceled Transfer$TransferState/Canceled
   :completed Transfer$TransferState/Completed
   :failed Transfer$TransferState/Failed
   :in-progress Transfer$TransferState/InProgress
   :waiting Transfer$TransferState/Waiting})

(defn- transfer-state->keywd [state]
  (get state=>keywd state))

(extend-type Transfer
  S3Transfer
  (done? [t]
    (.isDone t))
  (state [t]
    (transfer-state->keywd (.getState t)))
  (description [t]
    (.getDescription t))
  (wait-for-completion [t]
    (.waitForCompletion t))
  (wait-for-exception [t]
    (.waitForException t))
  ;; Wrap around TransferProgress
  (bytes-transfered [t]
    (.getBytesTransfered (transfer-progress t)))
  (percent-transfered [t]
    (.getPercentTransfered (transfer-progress t)))
  (total-bytes-to-transfer [t]
    (.getTotalBytesToTransfer (transfer-progress t)))
  (add-progress-listener [t listener]
    (.addProgressListener t listener)
    t))

(extend-type TransferManager
  S3Downloader
  (download!
    ([tm get-obj-req file]
       (.download tm get-obj-req (fileify file)))
    ([tm bucket key file]
       (.download tm bucket key (fileify file))))
  (download-directory!
    [tm bucket-name key-prefix dest-dir]
    (.downloadDirectory tm bucket-name key-prefix (fileify dest-dir)))
  S3Uploader
  (upload! [tm bucket key file]
    (.upload tm bucket key (fileify file)))
  (upload-string-as-file! [tm bucket key ^String string]
    (let [input-stream (ByteArrayInputStream. (.getBytes string "UTF-8"))
          str-len (long (count string))
          metadata (doto (ObjectMetadata. ) (.setContentLength str-len))]
      (.upload tm bucket key input-stream metadata)))
  (upload-directory! [tm bucket key-prefix local-dir recursive?]
    (.uploadDirectory tm
                      bucket
                      key-prefix
                      (fileify local-dir)
                      recursive?))
  (upload-files! [tm bucket key-prefix local-base-dir files]
    (.uploadFileList tm
                     bucket
                     key-prefix
                     (fileify local-base-dir)
                     (map fileify files)))
  S3TransferManager
  (shutdown-now! [tm]
    (.shutdownNow tm)))

(defmulti make-transfer-manager class)

(defmethod make-transfer-manager AWSCredentials [creds]
  (TransferManager. creds))
(defmethod make-transfer-manager AmazonS3 [s3]
  (TransferManager. s3))
(defmethod make-transfer-manager TransferManager [tm]
  tm)

;; This doesn't really make much sense... in retrospect, but I want to
;; keep it around for a while just incase I change my mind.
;;
;; (defmacro with-open-transfer
;;   "Given a TransferManager,
;; perform transfers and wait until all transfers are completed,
;; then shuts down"
;;   [tm & body]
;;   `(loop [tm# ~tm
;;           transfers# [~@body]]
;;      (if (every? done? transfers#)
;;        (shutdown-now tm#)
;;        (do
;;          (Thread/sleep 200)
;;          (recur tm# transfers#)))))


(def- progress-event=>keywd
  {ProgressEvent/CANCELED_EVENT_CODE :canceled
   ProgressEvent/COMPLETED_EVENT_CODE :completed
   ProgressEvent/FAILED_EVENT_CODE :failed
   ProgressEvent/STARTED_EVENT_CODE :started
   ProgressEvent/PART_COMPLETED_EVENT_CODE :part-completed
   ProgressEvent/PART_FAILED_EVENT_CODE :part-failed
   ProgressEvent/PART_STARTED_EVENT_CODE :part-started})

(defn- progress-event->keywd [event-code]
  (get progress-event=>keywd event-code))

(defmacro defprogress-listener
  "Macro to allow you to define a ProgressListener as though
you were defining a function that takes the number of bytes
transfered on the last event, as well as the event code,
mapped to particular keywords."
  [[bytes-transfered-binding event-code-binding] & body]
  `(reify ProgressListener
     (progressChanged [_ event#]
       (let [~bytes-transfered-binding (.getBytesTransfered event#)
             ~event-code-binding
             (progress-event->keywd (.getEventCode event#))]
         ~@body))))

(defmacro defprogress-change-listener
  "Like `progress-listener` but only performs body when an event change
occurs"
  [[bytes-transfered-binding event-code-binding] & body]
  `(reify ProgressListener
     (progressChanged [_ event#]
       (let [~bytes-transfered-binding (.getBytesTransfered event#)
             ~event-code-binding (progress-event->keywd (.getEventCode event#))]
         (when ~event-code-binding
           ~@body)))))
