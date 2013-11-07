(ns aws-clj-sdk.s3.core
  {:author "Alex Bahouth, Matt Halverson"
   :date "10/29/2013"}
  (:use clojure.java.io
        [clojure.set :only [difference]]
        roxxi.utils.print
        roxxi.utils.collections
        [clojure.string :only [replace-first]])
  (:require [aws-clj-sdk.s3.transfer :as t]
            [aws-clj-sdk.s3.client :as c]
            [aws-clj-sdk.s3.object :as obj]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Naming conventions within this file:
;;    - FILE or DIR means a java.io.File object
;;    - PATH, FILE-PATH, or DIR-PATH means the string representing
;;        a file or dir's location
;;    - relative (rel) and absolute (abs) are also used sometimes
;;
;; Also assumes you know what a 'BUCKET' and 'KEY' are (in Amazon S3 parlance).
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TODO: File utilities... should move out..
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn filename [file-path]
  (last (clojure.string/split file-path #"\/")))

(defn absolute-path [^java.io.File file]
  (.getAbsolutePath file))

(defn file-size [^java.io.File file]
  (.length file))

(defn directory [dir-path]
  (file dir-path))

(defn directory-contents [dir-path]
  ;; rest to pop off the directory itself
  (rest (file-seq (directory dir-path))))

(defn empty-directory? [dir-path]
  (empty? (directory-contents dir-path)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Helper functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- s3-and-local-fs-synced?
  "True, if all files are present in the local directory that prefix-match the
given key-prefix in s3, and the same number of bytes are present in the
local dir and S3.

Otherwise, false."
  [s3client bucket key-prefix local-dir-path]
  (let [s3-file-descs (c/object-descriptors s3client bucket key-prefix)
        s3-set (into #{} (map (comp filename obj/key) s3-file-descs))
        s3-size (apply + (map obj/content-length s3-file-descs))
        local-files (directory-contents local-dir-path)
        local-set (into #{} (map (comp filename absolute-path) local-files))
        local-size (apply + (map file-size local-files))]
    (and (empty? (difference s3-set local-set))
         (= s3-size local-size))))

(def all-files-uploaded? s3-and-local-fs-synced?)
(def all-files-downloaded? s3-and-local-fs-synced?)


(defn- determine-unsynced-files-to-download
  "Returns a seq of s3-keys (as strings) that ought to be re-downloaded"
  [s3client bucket key-prefix local-dir-path]
  (let [s3-file-descs (c/object-descriptors s3client bucket key-prefix)
        s3-filename=>desc (extract-map s3-file-descs
                                       :key-extractor (comp filename obj/key))
        lcl-filename=>size (extract-map (directory-contents local-dir-path)
                                        :key-extractor
                                        (comp filename absolute-path)
                                        :value-extractor file-size)
        ;; redownload the file
        ;; if we haven't yet downloaded a file that's in s3
        ;; or if the file is a different size in s3
        download? (fn [kv]
                    (or (nil? (lcl-filename=>size (key kv)))
                        (not= (obj/content-length (val kv))
                              (lcl-filename=>size (key kv)))))]
    (map #(obj/key (val %)) (filter download? s3-filename=>desc))))

(defn- make-end-with-slash [string]
  (if (.endsWith string "/")
    string
    (str string "/")))

;; ## I don't like repeating code, but I think it's the appropriate
;; thing to do here, since I think the api should be asymmetric
(defn- determine-unsynced-files-to-upload
  "Returns a seq of rel-paths (as strings) that ought to be re-uploaded"
  [s3client bucket key-prefix local-base-path rel-paths]
  (let [s3-file-descs (c/object-descriptors s3client bucket key-prefix)
        extract-rel-s3-path (comp #(replace-first % key-prefix "") obj/key)
        s3-rel-path=>desc (extract-map s3-file-descs
                                      :key-extractor extract-rel-s3-path)
        local-base-path-slash (make-end-with-slash local-base-path)
        calc-lcl-file-size (comp file-size file #(str local-base-path-slash %))
        lcl-rel-path=>size (extract-map rel-paths
                                       :value-extractor calc-lcl-file-size)
        upload? (fn [kv]
                  (or (nil? (s3-rel-path=>desc (key kv)))
                      (not= (obj/content-length (s3-rel-path=>desc (key kv)))
                            (val kv))))]
    (map #(key %) (filter upload? lcl-rel-path=>size))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; # Download town!
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; ## Forceful (over-writing) downloads

(defn download-files! [s3client bucket key-prefix local-dir-path]
  "For the set of files in s3 that prefix-match against key-prefix,
download all the files to the local filesystem, where their filenames
will have the same relative paths to local-dir-path as their keys
did to key-prefix.

If a file already exists on the local filesystem, it will be downloaded
again and overwritten."
  (let [tm (t/make-transfer-manager s3client)]
    (t/download-directory! tm bucket key-prefix local-dir-path)))

;; ## Non-forceful (non-over-writing) downloads

(defn download-sync-files! [s3client bucket key-prefix local-dir-path]
  "Like download-files!, except it DOESN'T waste time redownloading
files that are already synced."
  (cond
   (empty-directory? (directory local-dir-path))
   (download-files! s3client bucket key-prefix local-dir-path)

   (all-files-downloaded? s3client bucket key-prefix local-dir-path)
   :no-need-to-download

   :else
   (let [s3-files-to-download (determine-unsynced-files-to-download
                               s3client
                               bucket
                               key-prefix
                               (directory (str local-dir-path "/" key-prefix)))
         tm (t/make-transfer-manager s3client)]
     (map #(t/download! tm bucket % (file (str local-dir-path "/" %)))
          s3-files-to-download))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; # Upload town!
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; ## This relies on the fact that the amazon api returns the
;; object-summaries in alphabetical order (which Amazon's api
;; does guarantee).
;;
;; Only two cases, obviously:
;;   key doesn't exist in the bucket
;;   key exists
;;
;; but it's complicated by the fact that prefix-matching can occur
;; e.g. okl-danger/yodaetl is not a key, but it prefix-matches things
;; that are.
(defn present-in-s3? [s3client bucket key]
  "Answers the question, is there a value associated to 'key' in the given
bucket."
  (let [list-obj-req (obj/make-list-objects-request bucket key :max-keys 1)
        list-obj (c/list-objects-by-request s3client list-obj-req)
        obj-summary (first (obj/object-summaries list-obj))
        returned-key (and obj-summary (obj/key obj-summary))]
    (= key returned-key)))

;; ## Forceful (over-writing) uploads

(defn upload-file! [s3client bucket key local-path]
  "Upload the file at local-path to the specified key in s3.

Overwrite if the file was already in s3."
  (let [tm (t/make-transfer-manager s3client)]
    (t/upload! tm bucket key local-path)))

(defn upload-string-as-file! [s3client bucket key ^String string]
  "Like upload-file!, except you say what you want the file's contents
to be (by supplying them as a String).

Useful if you have a String that you want to upload, and you don't want
to bother landing it on your local file-system."
  (let [tm (t/make-transfer-manager s3client)]
    (t/upload-string-as-file! tm bucket key string)))

(defn upload-files! [s3client bucket key-prefix local-base-path local-rel-paths]
  "For the given vector/seq of local-rel-paths (based off of local-base-path),
upload all the files to s3, where their keys will have the same rel-paths (but
based off of key-prefix instead).

If a key was already present in s3, it will be overwritten."
  (let [tm (t/make-transfer-manager s3client)
        local-dir (directory local-base-path)
        files (map (comp file #(str local-base-path %)) local-rel-paths)]
    (t/upload-files! tm bucket key-prefix local-dir files)))

(defn upload-directory! [s3client bucket key-prefix local-dir-path recursive?]
  "Upload the directory at local-dir-path to s3, with keys relative to
key-prefix.

If a key was already present in s3, it will be overwritten."
  (let [tm (t/make-transfer-manager s3client)]
    (t/upload-directory! tm bucket key-prefix local-dir-path recursive?)))


;; ## Non-forceful uploads

(defn upload-file-if-not-there! [s3client bucket key local-path]
  "Like upload-file!, except it DOESN'T overwrite an existing file."
  (if (not (present-in-s3? s3client bucket key))
    (upload-file! s3client bucket key local-path)
    :no-need-to-upload))

(defn upload-sync-file! [s3client bucket key local-path]
  "If the file already exists in s3 AND is the same number of bytes
as the file at local-path, this is a no-op. Otherwise, uploads (possibly
overwriting) the file at local-path to the specified s3 bucket and key"
  (let [object-descriptors (c/object-descriptors s3client bucket key)
        s3-file-desc (first object-descriptors)]
    (if (or (nil? s3-file-desc)
            (not= (obj/content-length s3-file-desc)
                  (file-size (file local-path))))
      (upload-file! s3client bucket key local-path)
      :no-need-to-upload)))

(defn upload-sync-files!
  [s3client bucket key-prefix local-base-path local-rel-paths]
  "Like upload-files!, except it DOESN'T waste time reuploading files that
are already synced."
  (let [rel-paths-to-upload (determine-unsynced-files-to-upload s3client
                                                                bucket
                                                                key-prefix
                                                                local-base-path
                                                                local-rel-paths)
        abs-paths-to-upload (map #(str local-base-path %) rel-paths-to-upload)
        files-to-upload (map file abs-paths-to-upload)
        local-base-dir (directory local-base-path)
        tm (t/make-transfer-manager s3client)]
    (if (empty? files-to-upload)
      :no-need-to-upload
      (t/upload-files! tm bucket key-prefix local-base-dir files-to-upload))))

(defn upload-sync-directory! [s3client bucket key-prefix local-dir-path]
  "Like upload-directory!, except it DOESN'T waste time reuploading files that
are already synced."
  (let [files (directory-contents local-dir-path)
        abs-paths (map #(.getPath ^java.io.File %) files)
        rel-paths (map #(replace-first ^String % local-dir-path "") abs-paths)]
    (upload-sync-files! s3client bucket key-prefix local-dir-path rel-paths)))
