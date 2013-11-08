(ns aws-clj-sdk.s3.core-test
  (:require [aws-clj-sdk.s3.core :as s3]
            [aws-clj-sdk.s3.client :as s3c]
            [aws-clj-sdk.s3.transfer :as t]
            [aws-clj-sdk.s3.object :as obj]
            [aws-clj-sdk.auth.core-test :as auth])
  (:use clojure.java.io
        clojure.test
        roxxi.utils.print))

(defn make-s3client-from-creds []
  (s3c/make-s3-client
   (auth/make-creds-from-file auth/credential-file)))

(def s3client (make-s3client-from-creds))
(def file1 "conf/resources/s3/file1.txt")
(def file2 "conf/resources/s3/file2.txt")
(def test-dir "conf/resources/s3")

(defn get-test-bucket-name []
  "okl-danger-dev-mhalverson")
(def test-bucket (get-test-bucket-name))



(defmacro present? [file-name]
 `(s3/present-in-s3? s3client test-bucket ~file-name))

(def sandbox-dir "test/.sandbox/")
(def sandbox-file1 (str sandbox-dir "file1.txt"))
(def sandbox-file2 (str sandbox-dir "file2.txt"))
(defn- local-file-exists? [path]
  (.exists (file path)))

(defn- clean-sandbox! []
  (if (local-file-exists? sandbox-file1)
    (delete-file sandbox-file1))
  (if (local-file-exists? sandbox-file2)
    (delete-file sandbox-file2)))

(defmacro copy-file [src dest]
  `(copy (file ~src) (file ~dest)))

(defmacro synced? []
  `(s3/all-files-downloaded? s3client test-bucket "" sandbox-dir))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Download tests!
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest get-file-contents-test []
  (testing "put a file in s3, get the file contents, and it should be equal to
the contents of the file"
    (try
      (let [upload (s3/upload-file! s3client
                                    test-bucket
                                    "file1.txt"
                                    file1)]
        (t/wait-for-completion upload))
      (let [local-contents (slurp file1)
            s3-contents (s3/get-file-contents s3client
                                              test-bucket
                                              "file1.txt")]
        (is (= (count local-contents) (count s3-contents)))
        (is (= local-contents s3-contents))
        (is (= local-contents "file for upload tests\n")))
      (finally
        (s3c/delete-objects! s3client test-bucket ["file1.txt"])))))

(deftest all-files-downloaded?-test []
  (testing "put two files in s3, but don't download them yet. Should
return false. Download one, should return false. Create the second one
but make it the wrong size, shouuld still return false. Make the second
one the right size, then should return true."
    (try
      (clean-sandbox!)
      (let [upload (s3/upload-directory! s3client test-bucket "" "conf/resources/s3/" false)]
        (t/wait-for-completion upload)
        (is (not (synced?)))
        (copy-file file1 sandbox-file1)
        (is (not (synced?)))
        (copy-file file1 sandbox-file2) ;; NB file2 will be the wrong size.
        (is (not (synced?)))
        (copy-file file2 sandbox-file2)
        (is (synced?)))
      (finally
        (s3c/delete-objects! s3client test-bucket ["file1.txt" "file2.txt"])
        (clean-sandbox!)))))

(deftest download-files!-test []
  (testing "put 2 files in s3, but don't download them yet. Shouldn't be on
filesystem yet. Download directory, then both should be."
    (try
      (clean-sandbox!)
      (let [upload (s3/upload-directory! s3client test-bucket "" "conf/resources/s3/" false)]
        (t/wait-for-completion upload)
        (is (not (synced?))))
      (let [download (s3/download-files! s3client test-bucket "" sandbox-dir)]
        (t/wait-for-completion download)
        (is (synced?)))
      (finally
        (s3c/delete-objects! s3client test-bucket ["file1.txt" "file2.txt"])
        (clean-sandbox!)))))




(defmacro eager-map [f coll & args]
  `(dorun (map ~f ~coll ~@args)))

(deftest download-sync-files!-test []
  (testing "outcome is to have file1.txt and file2.txt in test/.sandbox/
download-sync-files! from nothing; should end up with both files
do it again from one file being there; should end up with both files
do it again from both files being there but one of them being the wrong size;
  should end up with both files
do it again from both files being there; should end up with both files still"
    (try
      (clean-sandbox!)
      (is (not (present? "file1.txt")))
      (is (not (present? "file2.txt")))
      (is (synced?))
      (let [upload (s3/upload-directory! s3client test-bucket "" "conf/resources/s3/" false)]
        (t/wait-for-completion upload)
        (is (not (synced?))))
      (let [down1 (s3/download-sync-files! s3client test-bucket "" sandbox-dir)]
        (when (not (keyword? down1))
          (t/wait-for-completion down1))
        (is (synced?))
        (clean-sandbox!))
      (copy-file file1 sandbox-file1)
      (is (not (synced?)))
      (let [down2 (s3/download-sync-files! s3client test-bucket "" sandbox-dir)]
        (when (not (keyword? down2))
          (eager-map t/wait-for-completion down2))
        (is (synced?))
        (clean-sandbox!))
      (copy-file file1 sandbox-file1)
      (copy-file file1 sandbox-file2) ;; NB file2 will be the wrong size.
      (is (not (synced?)))
      (let [down3 (s3/download-sync-files! s3client test-bucket "" sandbox-dir)]
        (when (not (keyword? down3))
          (eager-map t/wait-for-completion down3))
        (is (synced?)))
      (let [down4 (s3/download-sync-files! s3client test-bucket "" sandbox-dir)]
        (is (keyword? down4)))
      (finally
        (s3c/delete-objects! s3client test-bucket ["file1.txt" "file2.txt"])
        (clean-sandbox!)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Upload tests!
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest present-in-s3?-test []
  (testing "non-existent file"
    (is (not (present? "non-existent-file"))))

  (testing "file that exists (after we've uploaded it)"
    (try
      (is (not (present? "present-test")))
      (let [upload (s3/upload-file! s3client
                                    test-bucket
                                    "present-test"
                                    file1)]
        (t/wait-for-completion upload)
        (is (present? "present-test")))
      (finally
        (s3c/delete-object! s3client test-bucket "present-test")
        (is (not (present? "present-test"))))))

  (testing "the key is merely a prefix, and no file actually exists
for that key"
    (try
      (is (not (present? "file1")))
      (is (not (present? "file2")))
      (let [upload1 (s3/upload-file! s3client
                                     test-bucket
                                     "file1"
                                     file1)
            upload2 (s3/upload-file! s3client
                                     test-bucket
                                     "file2"
                                     file1)]
        (t/wait-for-completion upload1)
        (t/wait-for-completion upload2)
        (is (present? "file1"))
        (is (present? "file2"))
        (is (not (present? "file"))))
      (finally
        (s3c/delete-objects! s3client test-bucket ["file1" "file2"])
        (is (not (present? "file1")))
        (is (not (present? "file2"))))))

  (testing "the file exists, AND it's a prefix for other keys that also exist"
    (try
      (is (not (present? "file")))
      (is (not (present? "file-prefix")))
      (let [upload-exact (s3/upload-file! s3client
                                          test-bucket
                                          "file"
                                          file1)
            upload-prefix (s3/upload-file! s3client
                                           test-bucket
                                           "file-prefix"
                                           file1)]
        (t/wait-for-completion upload-exact)
        (t/wait-for-completion upload-prefix)
        (is (present? "file"))
        (is (present? "file-prefix")))
      (finally
        (s3c/delete-objects! s3client test-bucket ["file" "file-prefix"])
        (is (not (present? "file")))
        (is (not (present? "file-prefix")))))))

(deftest upload-file!-and-delete-file!-test []
  (testing "the file isn't there, then we upload it, then it's there"
    (try
      (is (not (present? "file")))
      (let [upload (s3/upload-file! s3client
                                    test-bucket
                                    "file"
                                    file1)]
        (t/wait-for-completion upload))
      (is (present? "file"))
      (finally
        (s3c/delete-object! s3client test-bucket "file")
        (is (not (present? "file")))))))

(deftest upload-directory!-test []
  (testing "the files aren't there, then we upload them, then they're there"
    (try
      (is (not (present? "prefix/file1.txt")))
      (is (not (present? "prefix/file2.txt")))
      (let [upload (s3/upload-directory! s3client
                                         test-bucket
                                         "prefix"
                                         test-dir
                                         false)]
        (t/wait-for-completion upload)
        (is (present? "prefix/file1.txt"))
        (is (present? "prefix/file2.txt")))
      (finally
        (s3c/delete-objects! s3client test-bucket ["prefix/file1.txt"
                                                   "prefix/file2.txt"])
        (is (not (present? "prefix/file1.txt")))
        (is (not (present? "prefix/file2.txt")))))))

(deftest upload-files!-test []
  (testing "the files aren't there, then we upload them, then they're there"
    (try
      (is (not (present? "s3/file1.txt")))
      (is (not (present? "s3/file2.txt")))
      (let [upload (s3/upload-files! s3client
                                     test-bucket
                                     ""
                                     "conf/resources/"
                                     ["s3/file1.txt" "s3/file2.txt"])]
        (t/wait-for-completion upload)
        (is (present? "s3/file1.txt"))
        (is (present? "s3/file2.txt")))
      (finally
        (s3c/delete-objects! s3client test-bucket ["s3/file1.txt"
                                                   "s3/file2.txt"])
        (is (not (present? "s3/file1.txt")))
        (is (not (present? "s3/file2.txt")))))))

(deftest upload-file-if-not-there!-test []
  (testing "the file isn't there, then we upload it, then we attempt
to re-upload a file to the same key (but it's a different size), and
the file doesn't change size in s3"
    (try
      (is (not (present? "file")))
      (let [upload1 (s3/upload-file-if-not-there! s3client
                                                  test-bucket
                                                  "file"
                                                  file1)]
        (t/wait-for-completion upload1)
        (is (present? "file"))
        (is (= (s3/file-size (file file1))
               (obj/content-length (first (s3c/object-descriptors s3client test-bucket "file"))))))
      (let [upload2 (s3/upload-file-if-not-there! s3client
                                                  test-bucket
                                                  "file"
                                                  file2)]
        (is (= upload2 :no-need-to-upload))
        (is (= (s3/file-size (file file1))
               (obj/content-length (first (s3c/object-descriptors s3client test-bucket "file")))))
        (is (not= (s3/file-size (file file1))
                  (s3/file-size (file file2)))))
      (finally
        (s3c/delete-object! s3client test-bucket "file")))))

(deftest upload-string-as-file!-test []
  (testing "the file isn't there, then we upload the string as that file,
then the file is there and it's the same number of bytes as the string"
    (try
      (is (not (present? "file")))
      (let [string "Watson, do you read me?"
            num-bytes (count (.getBytes string))
            upload (s3/upload-string-as-file! s3client test-bucket "file" string)]
        (t/wait-for-completion upload)
        (is (present? "file"))
        (is (= num-bytes
               (obj/content-length (first (s3c/object-descriptors s3client test-bucket "file"))))))
      (finally
        (s3c/delete-object! s3client test-bucket "file")))))

(deftest upload-sync-file!-test []
  (testing "the file isn't there, then we upload it, then we
re-upload a file to the same key (but it's a different size), and
the file changes size in s3, then we attempt to re-upload again
and it's a no-op"
    (try
      (is (not (present? "file")))
      (let [upload1 (s3/upload-sync-file! s3client
                                          test-bucket
                                          "file"
                                          file1)]
        (t/wait-for-completion upload1)
        (is (present? "file"))
        (is (= (s3/file-size (file file1))
               (obj/content-length (first (s3c/object-descriptors s3client test-bucket "file"))))))
      (let [upload2 (s3/upload-sync-file! s3client
                                          test-bucket
                                          "file"
                                          file2)]
        (t/wait-for-completion upload2)
        (is (= (s3/file-size (file file2))
               (obj/content-length (first (s3c/object-descriptors s3client test-bucket "file")))))
        (is (not= (s3/file-size (file file1))
                  (s3/file-size (file file2)))))
      (let [upload3 (s3/upload-sync-file! s3client
                                          test-bucket
                                          "file"
                                          file2)]
        (is (= upload3 :no-need-to-upload)))
      (finally
        (s3c/delete-object! s3client test-bucket "file")))))

(deftest upload-sync-files!-test []
  (testing "outcome is to have file1.txt and file2.txt from test/.sandbox/ in s3
upload-sync-files! from nothing; should end up with both files
do it again from one file being there; should end up with both files
do it again from both files being there but one of them being the wrong size;
  should end up with both files
do it again from both files being there; should end up with both files still"
    (try
      (copy-file file1 sandbox-file1)
      (copy-file file2 sandbox-file2)
      (is (not (present? "file1.txt")))
      (is (not (present? "file2.txt")))
      (is (not (synced?)))
      (let [up1 (s3/upload-sync-files! s3client
                                       test-bucket
                                       ""
                                       sandbox-dir
                                       ["file1.txt" "file2.txt"])]
        (t/wait-for-completion up1)
        (is (synced?))
        (s3c/delete-objects! s3client test-bucket ["file2.txt"]))
      (is (present? "file1.txt"))
      (is (not (present? "file2.txt")))
      (is (not (synced?)))
      (let [up2 (s3/upload-sync-files! s3client
                                       test-bucket
                                       ""
                                       sandbox-dir
                                       ["file1.txt" "file2.txt"])]
        (t/wait-for-completion up2)
        (is (synced?))
        (t/wait-for-completion
         (s3/upload-file! s3client test-bucket "file1.txt" sandbox-file2))) ;; NB will be the wrong size
      (is (present? "file1.txt"))
      (is (present? "file2.txt"))
      (is (not (synced?)))
      (let [up3 (s3/upload-sync-files! s3client
                                       test-bucket
                                       ""
                                       sandbox-dir
                                       ["file1.txt" "file2.txt"])]
        (t/wait-for-completion up3)
        (is (synced?)))
      (let [up4 (s3/upload-sync-files! s3client
                                       test-bucket
                                       ""
                                       sandbox-dir
                                       ["file1.txt" "file2.txt"])]
        (is (= :no-need-to-upload up4))
        (is (synced?)))
      (finally
        (s3c/delete-objects! s3client test-bucket ["file1.txt" "file2.txt"])
        (clean-sandbox!)))))


(deftest upload-sync-directory!-test []
  (testing "outcome is to have file1.txt and file2.txt from test/.sandbox/ in s3
upload-sync-directory! from nothing; should end up with both files
do it again from one file being there; should end up with both files
do it again from both files being there but one of them being the wrong size;
  should end up with both files
do it again from both files being there; should end up with both files still"
    (try
      (copy-file file1 sandbox-file1)
      (copy-file file2 sandbox-file2)
      (is (not (present? "file1.txt")))
      (is (not (present? "file2.txt")))
      (is (not (synced?)))
      (let [up1 (s3/upload-sync-directory! s3client
                                           test-bucket
                                           ""
                                           sandbox-dir)]
        (t/wait-for-completion up1)
        (is (synced?))
        (s3c/delete-objects! s3client test-bucket ["file2.txt"]))
      (is (present? "file1.txt"))
      (is (not (present? "file2.txt")))
      (is (not (synced?)))
      (let [up2 (s3/upload-sync-directory! s3client
                                           test-bucket
                                           ""
                                           sandbox-dir)]
        (t/wait-for-completion up2)
        (is (synced?))
        (t/wait-for-completion
         (s3/upload-file! s3client test-bucket "file1.txt" sandbox-file2))) ;; NB will be the wrong size
      (is (present? "file1.txt"))
      (is (present? "file2.txt"))
      (is (not (synced?)))
      (let [up3 (s3/upload-sync-directory! s3client
                                           test-bucket
                                           ""
                                           sandbox-dir)]
        (t/wait-for-completion up3)
        (is (synced?)))
      (let [up4 (s3/upload-sync-directory! s3client
                                           test-bucket
                                           ""
                                           sandbox-dir)]
        (is (= :no-need-to-upload up4))
        (is (synced?)))
      (finally
        (s3c/delete-objects! s3client test-bucket ["file1.txt" "file2.txt"])
        (clean-sandbox!)))))
