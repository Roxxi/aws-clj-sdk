(ns aws-clj-sdk.s3.core
  {:author "Alex Bahouth, Matt Halverson"
   :date "10/29/2013"}
  (:use clojure.java.io
        [clojure.set :only [difference]]
        roxxi.utils.print
        roxxi.utils.collections)
  (:require [aws-clj-sdk.s3.transfer :as t]
            [aws-clj-sdk.s3.client :as c]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TODO: File utilities... should move out..
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn filename [filepath]
  (last (clojure.string/split filepath #"\/")))

(defn absolute-path [^java.io.File file]
  (.getAbsolutePath file))

(defn file-size [^java.io.File file]
  (.length file))

(defn directory [dir-path]
  (file dir-path))

(defn directory-contents [dir]
  ;; rest to pop off the directory itself
  (rest (file-seq (directory dir))))

(defn empty-directory? [dir]
  (empty? (directory-contents dir)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; # Download town!
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn all-files-downloaded?
  "True, if all files are present in the local directory that are listed in S3,
and the same number of bytes have been downloaded as in S3"
  [s3client bucket prefix local-dir]
  (let [s3-file-descs (c/object-descriptors s3client bucket prefix)
        s3-set (into #{} (map (comp filename c/key) s3-file-descs))
        s3-size (apply + (map c/content-length s3-file-descs))
        local-files (directory-contents local-dir)
        local-set (into #{} (map (comp filename absolute-path) local-files))
        local-size (apply + (map file-size local-files))]
    (and (empty? (difference s3-set local-set))
         (= s3-size local-size))))

(defn download-directory! [s3client bucket prefix local-path]
  (let [tm (t/make-transfer-manager s3client)]
    (t/download-directory! tm bucket prefix local-path)))

(defn- determine-missing-files [s3client bucket prefix local-dir]
  (let [s3-file-descs (c/object-descriptors s3client bucket prefix)
        s3-filename=>desc (extract-map s3-file-descs
                                       :key-extractor (comp filename c/key))
        lcl-filename=>size (extract-map (directory-contents local-dir)
                                        :key-extractor
                                        (comp filename absolute-path)
                                        :value-extractor file-size)
        ;; redownload the file
        ;; if we haven't yet downloaded a file that's in s3
        ;; or if the file in S3 is bigger than the file here
        download? (fn download? [kv]
                    (or (nil? (lcl-filename=>size (key kv)))
                        (> (c/content-length (val kv))
                           (lcl-filename=>size (key kv)))))
        s3-keys-to-download
        (map #(c/key (val %)) (filter download? s3-filename=>desc))]
    s3-keys-to-download))

(defn download-sync-files! [s3client bucket prefix local-path]
  (let [s3-files-to-download (determine-missing-files
                              s3client bucket prefix (directory (str local-path "/" prefix)))
        tm (t/make-transfer-manager s3client)]
    (map #(t/download! tm bucket % (file (str local-path "/" %)))
         s3-files-to-download)))



(defn download-sync-directory!
  "Download all the files in a particular s3 bucket/path if they
haven't already been completely downloaded.

Useful if a connection is faulty or interrupted, because completed
files will not be redownloaded"
  [s3client bucket path local-path & {:keys [file-prefix]}]
  (let [local-dir (directory (str local-path "/" path))
        prefix (if file-prefix (str path "/" file-prefix) path)]
    (if (empty-directory? local-dir)
      (download-directory! s3client bucket prefix local-path))
      (if (all-files-downloaded? s3client bucket prefix local-dir)
        :done
        (download-sync-files! s3client bucket prefix local-path))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; # Upload town!
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; ## This relies on the fact that the amazon api returns the
;; object-summaries in alphabetical order (which Amazon's api
;; does guarantee).
;;
;; Two cases:
;;   key doesn't exist in the bucket
;;   key exists
;; but it's complicated by the fact that prefix-matching can occur
;; e.g. okl-danger/yodaetl is not a key, but it prefix-matches things
;; that are.
(defn present-in-s3? [s3client bucket key]
  "Answers the question, is there a value associated to 'key' in the given
bucket."
  (let [list-obj-req (c/make-list-objects-request bucket key :max-keys 1)
        list-obj (c/list-objects-by-request s3client list-obj-req)
        obj-summary (first (c/object-summaries-from-object-listing list-obj))
        returned-key (and obj-summary (c/key obj-summary))]
    (= key returned-key)))

(defn upload-file! [s3client bucket key local-path]
  (let [tm (t/make-transfer-manager s3client)]
    (t/upload! tm bucket key local-path)))

(defn upload-directory! [s3client bucket dir-key-prefix local-dir recursive?]
  (let [tm (t/make-transfer-manager s3client)]
    (t/upload-directory! tm bucket dir-key-prefix local-dir recursive?)))

(defn upload-files! [s3client bucket dir-key-prefix local-dir files]
  (let [tm (t/make-transfer-manager s3client)]
    (t/upload-files! tm bucket dir-key-prefix local-dir files)))


(defn upload-file-if-not-there! [s3client bucket key local-path]
  "If the file already exists in s3, this is a no-op. Otherwise, uploads
local-file to the specified s3 bucket and key"
  (if (not (present-in-s3? s3client bucket key))
    (upload-file! s3client bucket key local-path)
    :no-need-to-upload))


;; (defn upload-string-as-file! [s3client bucket key string]
;;   "At the specified bucket/key, creates a file whose contents are 'string'"
;;   (let [stream (input-stream string)]
;;     (upload-file s3client bucket key stream)


(defn upload-sync-file! [s3client bucket key local-path]
  "If the file already exists in s3 AND is the same number of bytes
as the file at local-path, this is a no-op. Otherwise, uploads (possibly
overwriting) local-file to the specified s3 bucket and key"
  (let [object-descriptors (c/object-descriptors s3client bucket key)
        s3-file-desc (first object-descriptors)]
    (if (or (nil? s3-file-desc)
            (not= (c/content-length s3-file-desc)
                  (file-size (file local-path))))
      (upload-file! s3client bucket key local-path)
      :no-need-to-upload)))


;; (defn all-files-uploaded? [] nil)

;; (defn upload-sync-files! [] nil)

;; (defn upload-sync-directory! [s3client bucket ])
