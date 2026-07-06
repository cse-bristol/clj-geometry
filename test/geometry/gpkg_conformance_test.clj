(ns geometry.gpkg-conformance-test
  "Runs the GeoPackage abstract test suite (geometry.gpkg.validate) against
   GeoPackages produced by geometry.gpkg/write, asserting our writer emits
   conformant files, plus a couple of negative tests that corrupt a file and
   assert the relevant checks flip to :fail."
  (:require [geometry.gpkg :as gpkg]
            [geometry.gpkg.validate :as v]
            [geometry.core :as g]
            [geometry.testutils :as tu]
            [clojure.java.io :as io]
            [clojure.test :as t]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [java.nio ByteBuffer]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defmacro with-temp-file [file & body]
  `(let [~file (.toFile (Files/createTempFile
                         "gpkg-conformance" ".gpkg"
                         (into-array FileAttribute [])))]
     (try ~@body
          (finally (io/delete-file ~file true)))))

(defn write-sample!
  "Write a sample GeoPackage exercising features (mixed geometry types, an
   auto integer PK, and the gpkg_schema extension), a spatial index (the
   gpkg_rtree_index extension), and a non-spatial attributes table."
  [file]
  (gpkg/write
   file "places"
   [{"geometry" (g/make-point 350000 400000)                 "name" "p" "cat" 1}
    {"geometry" (g/make-line-string [[0 0] [10 10] [20 5]])  "name" "l" "cat" 2}
    {"geometry" (tu/square 100 100 50)                        "name" "s" "cat" 1}
    {"geometry" nil                                           "name" "z" "cat" 2}]
   :schema
   {"geometry" {:type :geometry :srid 27700}
    "name"     {:type :string}
    "cat"      {:type :integer :constraint "cats"}}
   :constraints [{:name "cats" :type :enum :values ["1" "2"]}]
   :add-spatial-index true)

  (gpkg/write
   file "notes"
   [{"id" 1 "body" "first"}
    {"id" 2 "body" "second"}]
   :schema
   {"id"   {:type :integer :primary-key true}
    "body" {:type :string}}))

(defn results-by-req [file]
  (into {} (map (juxt :req identity)) (v/validate file)))

(t/deftest sample-is-conformant
  (with-temp-file f
    (write-sample! f)
    (let [results (v/validate f)
          fails   (filter #(= :fail (:status %)) results)]
      (t/is (empty? fails)
            (str "abstract test failures: "
                 (mapv (juxt :req :id :message) fails)))
      (t/testing "every check ran to a definite status"
        (t/is (every? #{:pass :fail :not-applicable :skip} (map :status results)))))))

(t/deftest key-requirements-pass
  (with-temp-file f
    (write-sample! f)
    (let [by-req (results-by-req f)]
      (t/testing "core"
        (doseq [req [1 2 3 5 6 7 10 11 12 13 14 15 16 17]]
          (t/is (= :pass (:status (by-req req)))
                (str "req " req ": " (:message (by-req req))))))
      (t/testing "features"
        (doseq [req [18 19 152 20 21 22 23 24 25 26 146 27 28 29 30 31 32 33]]
          (t/is (= :pass (:status (by-req req)))
                (str "req " req ": " (:message (by-req req))))))
      (t/testing "extensions (rtree + gpkg_schema present)"
        (doseq [req [58 60 61 62 63 64]]
          (t/is (= :pass (:status (by-req req)))
                (str "req " req ": " (:message (by-req req)))))
        (t/is (= :skip (:status (by-req 59)))))
      (t/testing "attributes"
        (t/is (= :pass (:status (by-req 118)))
              (:message (by-req 118)))))))

(t/deftest not-applicable-without-features
  (with-temp-file f
    (gpkg/write f "notes"
                [{"id" 1 "body" "x"}]
                :schema {"id"   {:type :integer :primary-key true}
                         "body" {:type :string}})
    (let [by-req (results-by-req f)]
      (t/testing "feature checks are not-applicable when there are no feature tables"
        (doseq [req [18 19 21 29 32]]
          (t/is (= :not-applicable (:status (by-req req)))
                (str "req " req))))
      (t/testing "attributes check still passes"
        (t/is (= :pass (:status (by-req 118))))))))

;; --- negative tests: the validator must catch a broken file ---

(t/deftest detects-bad-application-id
  (with-temp-file f
    (write-sample! f)
    ;; the SQLite application_id is a 4-byte big-endian int at offset 68
    (with-open [ch (java.nio.channels.FileChannel/open
                    (.toPath f)
                    (into-array java.nio.file.OpenOption
                                [java.nio.file.StandardOpenOption/WRITE]))]
      (.write ch (doto (ByteBuffer/allocate 4) (.putInt 0) (.flip)) 68))
    (let [by-req (results-by-req f)]
      (t/is (= :fail (:status (by-req 2)))
            "application_id check should fail on a zeroed application_id"))))

(t/deftest detects-bad-geometry-magic
  ;; no spatial index here, so corrupting the blob won't fire the rtree
  ;; triggers (which need ST_* functions registered per-connection).
  (with-temp-file f
    (gpkg/write f "places"
                [{"geometry" (g/make-point 350000 400000) "name" "p"}]
                :schema {"geometry" {:type :point :srid 27700}
                         "name"     {:type :string}})
    ;; corrupt the geometry blob's magic byte directly in SQLite
    (with-open [conn (jdbc/get-connection {:jdbcUrl (str "jdbc:sqlite:" f)})]
      (let [{:keys [fid g]} (jdbc/execute-one!
                             conn ["SELECT fid, geometry AS g FROM places
                                     WHERE geometry IS NOT NULL LIMIT 1"]
                             {:builder-fn rs/as-unqualified-lower-maps})]
        (aset-byte g 0 (byte 0x00))
        (jdbc/execute! conn ["UPDATE places SET geometry = ? WHERE fid = ?" g fid])))
    (let [by-req (results-by-req f)]
      (t/is (= :fail (:status (by-req 19)))
            "GPB blob format check should fail on a bad magic byte"))))
