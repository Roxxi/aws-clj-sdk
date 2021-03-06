(ns aws-clj-sdk.s3.client
  (:use clojure.java.io
        roxxi.utils.print
        roxxi.utils.collections)
  (:import [java.util Scanner]
           [com.amazonaws ClientConfiguration]
           [com.amazonaws.auth AWSCredentials]
           [com.amazonaws.services.s3 AmazonS3Client]
           [com.amazonaws.services.s3.model
            ObjectListing
            DeleteObjectsRequest
            DeleteObjectsRequest$KeyVersion])
  ;; for interactive development
  (:require [aws-clj-sdk.s3.object :as obj]))


;; For now, I'm just going to implement the functions I'm actually going
;; to end up using, since there would be way, way too many funcitons to
;; implement for completeness.
;;
;; (That's what pull requests are for anyhow.)


(defprotocol S3Client
  (object-contents [s3c bucket-name key]
    "Gets the contents of the file at the specified key.")
  (object-metadata [s3c bucket-name key]
    "Gets the metadata for the specified Amazon S3
object without actually fetching the object itself.")
  (list-objects-by-request [s3c list-objects-request]
    "Returns a list of summary information about the objects in
the given list-objects-request.")
  (list-objects [s3c bucket-name prefix]
    "Returns a list of summary information about the objects in
the specified bucket.")
  (object-summaries [s3c bucket-name prefix]
    "Gets the list of object summaries describing the objects
stored in the S3 bucket.")
  (object-keys [s3c bucket-name prefix]
    "Returns a S3 bucket key corresponding to each object stored
in the S3 bucket.")
  (object-descriptors [s3c bucket-name prefix]
    "Returns a collection of object-metadata corresponding to each
object stored in the S3 bucket.")
  (delete-object! [s3c bucket-name key]
    "Deletes the specified key from the specified bucket.")
  (delete-objects! [s3c bucket-name key-vector]
    "Deletes every key in the specified vector of keys
from the specified bucket."))



(defn- make-key-version [key]
 (DeleteObjectsRequest$KeyVersion. key))

(defn- make-delete-objects-request [bucket-name key-vec]
  (let [del-req (DeleteObjectsRequest. bucket-name)
        key-version-vec (map make-key-version key-vec)]
    (.withKeys del-req key-version-vec)))



(extend-type AmazonS3Client
  S3Client
  (object-contents [s3c bucket-name key]
    (let [obj-in-mem (.getObject s3c bucket-name key)
          input-stream (.getObjectContent obj-in-mem)
          scanner (doto (Scanner. input-stream "UTF-8") (.useDelimiter "\\A"))]
      (if (.hasNext scanner)
        (.next scanner)
        "")))
  (object-metadata [s3c bucket-name key]
    (.getObjectMetadata s3c bucket-name key))
  (list-objects-by-request [s3c list-objects-request]
    (.listObjects s3c list-objects-request))
  (list-objects [s3c bucket-name prefix]
    (.listObjects s3c bucket-name prefix))
  (object-summaries [s3c bucket-name prefix]
    (let [^ObjectListing listing (list-objects s3c bucket-name prefix)]
      (obj/object-summaries listing)))
  (object-keys [s3c bucket-name prefix]
     (map #(obj/key %) (object-summaries s3c bucket-name prefix)))
  (object-descriptors [s3c bucket-name prefix]
    (let [summaries (object-summaries s3c bucket-name prefix)]
      (map obj/make-s3-obj-desc
           summaries
           (map #(object-metadata s3c bucket-name %)
                (map #(obj/key %) summaries)))))
  (delete-object! [s3c bucket-name key]
    (delete-objects! s3c bucket-name [key]))
  (delete-objects! [s3c bucket-name key-vector]
    (let [delete-req (make-delete-objects-request bucket-name key-vector)]
      (.deleteObjects s3c delete-req))))


(defn make-s3-client
  ([^AWSCredentials credentials]
     (AmazonS3Client. credentials))
  ([^AWSCredentials credentials ^ClientConfiguration client-config]
     (AmazonS3Client. credentials client-config)))

;; ## See http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html?com/amazonaws/services/s3/model/ListObjectsRequest.html
