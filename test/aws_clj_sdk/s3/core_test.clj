(ns aws-clj-sdk.s3.core-test
  (:require [clj-yaml.core :as yaml]
            [aws-clj-sdk.s3.core :as s3]
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

;; in this ns
;; (def a (s3/upload-file (s3client) "okl-danger-dev-mhalverson" "sub_dir/matt_test" file-to-upload))

(defn get-test-bucket-name []
  "okl-danger-dev-mhalverson")
(def test-bucket (get-test-bucket-name))

(defmacro present? [file-name]
 `(s3/present-in-s3? s3client test-bucket ~file-name))

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
