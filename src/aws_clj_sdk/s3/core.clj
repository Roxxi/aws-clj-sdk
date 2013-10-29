(ns aws-clj-sdk.s3.core
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

(defn download-directory [s3client bucket prefix local-path]
  (let [tm (t/make-transfer-manager s3client)]
    (t/download-directory tm bucket prefix local-path)))

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

(defn download-sync-files [s3client bucket prefix local-path]
  (let [s3-files-to-download (determine-missing-files
                              s3client bucket prefix (directory (str local-path "/" prefix)))
        tm (t/make-transfer-manager s3client)]    
    (map #(t/download tm bucket % (file (str local-path "/" %)))
         s3-files-to-download)))



(defn download-sync-directory
  "Download all the files in a particular s3 bucket/path if they 
haven't already been completely downloaded.

Useful if a connection is faulty or interrupted, because completed 
files will not be redownloaded"
  [s3client bucket path local-path & {:keys [file-prefix]}]
  (let [local-dir (directory (str local-path "/" path))
        prefix (if file-prefix (str path "/" file-prefix) path)]
    (if (empty-directory? local-dir)
      (download-directory s3client bucket prefix local-path))
      (if (all-files-downloaded? s3client bucket prefix local-dir)
        :done
        (download-sync-files s3client bucket prefix local-path))))
