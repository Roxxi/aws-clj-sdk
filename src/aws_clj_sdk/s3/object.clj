(ns aws-clj-sdk.s3.object
  {:author "Alex Bahouth, Matt Halverson"
   :date "10/31/2013"}
  (:use roxxi.utils.print
        roxxi.utils.collections)
  (:import [com.amazonaws.services.s3.model
            ObjectListing
            ObjectMetadata
            S3ObjectSummary
            ListObjectsRequest]))

(defprotocol S3ObjSummary
  (key [summary]
    "Gets the key under which this object is stored in Amazon S3."))

(extend-type S3ObjectSummary
  S3ObjSummary
  (key [summary]
    (.getKey summary)))


(defprotocol S3ObjectListing
  (object-summaries [obj-listing]
    "Given an ObjectListing, gets the ObjectSummaries from it"))

(extend-type ObjectListing
  S3ObjectListing
  (object-summaries [listing]
    (.getObjectSummaries listing)))


(defprotocol S3ObjectMetadata
  (content-length [md]
        "Gets the Content-Length HTTP header indicating the size
of the associated object in bytes."))



(defrecord S3ObjectDescriptor [^S3ObjectSummary summary
                               ^ObjectMetadata metadata]
  S3ObjSummary ;; <- Note, this is our protocol
  (key [desc]
    (key summary))
  S3ObjectMetadata
  (content-length [desc]
    (.getContentLength metadata)))

(defn make-s3-obj-desc [summary metadata]
  (S3ObjectDescriptor. summary metadata))




(defn make-list-objects-request [bucket-name prefix & {:keys [marker
                                                              delimiter
                                                              max-keys]
                                                       :or {marker nil
                                                            delimiter nil
                                                            max-keys 1000}}]
  (ListObjectsRequest. bucket-name prefix marker delimiter (int max-keys)))
