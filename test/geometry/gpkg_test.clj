(ns geometry.gpkg-test
  (:require [geometry.gpkg :as sut]
            [clojure.test :as t]
            [clojure.java.io :as io]
            [geometry.core :as g])
  (:import [org.geotools.geometry.jts Geometries]
           [clojure.lang IReduceInit]))

(t/deftest test-spatial-roundtrip
  (let [f (.toFile (java.nio.file.Files/createTempFile
                    "test-read-write" ".gpkg"
                    (into-array java.nio.file.attribute.FileAttribute [])))]
    (try
      ;; write some features down
      (sut/write
       f
       "test-table"

       [{"geometry" (g/make-point 1 2) "id" 1 "b" "abc" :c true :inf ##Inf}
        {"geometry" (g/make-point 4 5) "id" 2 "b" "def" :c false :inf ##Inf}
        {"geometry" nil "id" 3 "b" "ghi" :c false :inf ##Inf}]

       :schema
       {"geometry" {:type :point :srid 27700}
        "id" {:type :integer}
        "b" {:type :string}
        "c" {:type :boolean :accessor :c}
        "inf" {:type :double :accessor :inf}})

      ;; read them back and compare

      (with-open [in (sut/open f :table-name "test-table")]
        (t/is (= (set [{:geometry nil "id" 3 :table "test-table" :crs "EPSG:27700" "b" "ghi" "c" 0 "inf" ##Inf}
                       {:geometry (g/make-point 1 2) "id" 1 :table "test-table" :crs "EPSG:27700" "b" "abc" "c" 1 "inf" ##Inf}
                       {:geometry (g/make-point 4 5) "id" 2 :table "test-table" :crs "EPSG:27700" "b" "def" "c" 0 "inf" ##Inf}])

                 ;; we map into {} to strip off the feature type
                 ;; since we want to do a simple comparison here
                 (set (map #(into {} %) (sut/features in))))))

      (catch Exception e (prn e) (throw e))
      (finally (io/delete-file f)))))

(t/deftest test-can-read-twice
  (let [f (.toFile (java.nio.file.Files/createTempFile
                    "test-read-write" ".gpkg"
                    (into-array java.nio.file.attribute.FileAttribute [])))]
    (try
      (sut/write
       f "test-table"
       [{"geometry" (g/make-point 1 2) "id" 1}
        {"geometry" (g/make-point 4 5) "id" 2}]

       :schema
       {"geometry" {:type :point :srid 27700}
        "id" {:type :integer}})

      (with-open [in (sut/open f :table-name "test-table")]
        (let [ls (sut/features in)]
          (t/is (= [{:geometry (g/make-point 1 2) "id" 1 :table "test-table" :crs "EPSG:27700"}
                    {:geometry (g/make-point 4 5) "id" 2 :table "test-table" :crs "EPSG:27700"}]
                   (vec (map #(into {} %) ls))))
          (t/is (= [{:geometry (g/make-point 1 2) "id" 1 :table "test-table" :crs "EPSG:27700"}
                    {:geometry (g/make-point 4 5) "id" 2 :table "test-table" :crs "EPSG:27700"}]
                   (vec (map #(into {} %) ls))))))

      (catch Exception e (prn e) (throw e))
      (finally (io/delete-file f)))))

(t/deftest test-non-spatial-roundtrip
  (let [f (.toFile (java.nio.file.Files/createTempFile
                    "test-read-write" ".gpkg"
                    (into-array java.nio.file.attribute.FileAttribute [])))]
    (try
      ;; write some non-spatial data:
      (sut/write
       f
       "sqlite-table"

       [{:a 1 "b" "aaa" :c true :inf ##Inf}
        {:a 2 "b" "bbb" :c false :inf ##Inf}]

       :schema
       {"a" {:type Integer :accessor :a}
        "b" {:type String}
        "c" {:type Boolean :accessor :c}
        "inf" {:type :double :accessor :inf}})

      ;; read the non-spatial data:
      (with-open [in (sut/open f :table-name "sqlite-table")]
        (t/is (= [{:geometry nil "a" 1 "b" "aaa" "c" 1 "inf" ##Inf :table "sqlite-table" :crs nil}
                  {:geometry nil "a" 2 "b" "bbb" "c" 0 "inf" ##Inf :table "sqlite-table" :crs nil}]
                 (vec (map #(into {} %) (sut/features in))))))

      (catch Exception e (prn e) (throw e))
      (finally (io/delete-file f)))))

(t/deftest test-mixed-roundtrip
  (let [f (.toFile (java.nio.file.Files/createTempFile
                    "test-read-write" ".gpkg"
                    (into-array java.nio.file.attribute.FileAttribute [])))]
    (try
      ;; write some features down
      (sut/write
       f
       "test-table"

       [{"geometry" (g/make-point 1 2) "id" 1}
        {"geometry" (g/make-point 4 5) "id" 2}]

       :schema
       {"geometry" {:type :point :srid 27700}
        "id" {:type Integer}})

      ;; write some non-spatial data:
      (sut/write
       f
       "data-table"

       [{:a 1 "b" "aaa" :c true :inf ##Inf}
        {:a 2 "b" "bbb" :c false :inf ##Inf}]

       :schema
       {"a" {:type Integer :accessor :a}
        "b" {:type String}
        "c" {:type Boolean :accessor :c}
        "inf" {:type :double :accessor :inf}})

      (with-open [in (sut/open f :spatial-only? false :key-transform keyword)]
        (t/is (= [{:geometry (g/make-point 1 2) :id 1 :table "test-table" :crs "EPSG:27700"}
                  {:geometry (g/make-point 4 5) :id 2 :table "test-table" :crs "EPSG:27700"}
                  {:geometry nil :a 1 :b "aaa" :c 1 :inf ##Inf :table "data-table" :crs nil}
                  {:geometry nil :a 2 :b "bbb" :c 0 :inf ##Inf :table "data-table" :crs nil}]
                 (vec (map #(into {} %) (sut/features in))))))

      (catch Exception e (prn e) (throw e))
      (finally (io/delete-file f)))))

(t/deftest test-empty-read-writes
  (let [f (.toFile (java.nio.file.Files/createTempFile
                    "test-read-write" ".gpkg"
                    (into-array java.nio.file.attribute.FileAttribute [])))]
    (try
      ;; write some features down
      (sut/write f "test-table" []
       :schema
       {"geometry" {:type :point :srid 27700}
        "id" {:type Integer}})

      ;; write some non-spatial data:
      (sut/write f "data-table" []
       :schema
       {"a" {:type Integer :accessor :a}
        "b" {:type String}
        "c" {:type Boolean :accessor :c}
        "inf" {:type :double :accessor :inf}})

      (with-open [in (sut/open f :spatial-only? false :key-transform keyword)]
        (t/is (= []
                 (vec (map #(into {} %) (sut/features in))))))

      (catch Exception e (prn e) (throw e))
      (finally (io/delete-file f)))))

(t/deftest test-table-names
 (let [f (.toFile (java.nio.file.Files/createTempFile
                    "test-read-write" ".gpkg"
                    (into-array java.nio.file.attribute.FileAttribute [])))]
    (try
      (sut/write
       f
       "test-table"
       [{"geometry" (g/make-point 1 2) "id" 1}]

       :schema
       {"geometry" {:type :point :srid 27700}
        "id" {:type Integer}})

      (sut/write
       f
       "data-table"
       [{:a 2 "b" "bbb" :c false :inf ##Inf}]

       :schema
       {"a" {:type Integer :accessor :a}
        "b" {:type String}
        "c" {:type Boolean :accessor :c}
        "inf" {:type :double :accessor :inf}})

      (t/is (= #{"test-table" "data-table"}
               (sut/table-names f)))

      (t/is (= #{"test-table"}
               (sut/table-names f :spatial-only? true)))

      (catch Exception e (prn e) (throw e))
      (finally (io/delete-file f)))))

(t/deftest test-empty-table
  (let [f (.toFile (java.nio.file.Files/createTempFile
                    "test-empty-table" ".gpkg"
                    (into-array java.nio.file.attribute.FileAttribute [])))]
    (sut/write
     f "zempty" nil {:schema {"bork" {:type :integer}}})

    (sut/write
     f "full" [{:bork 1} {:bork 2} {:bork 9}] {:schema {"bork" {:type :integer :accessor :bork}}})

    (sut/write
     f "aempty" [] {:schema {"bork" {:type :integer}}})

    (let [in (with-open [g (sut/open f :key-transform keyword)] (doall (sut/features g)))]
      (t/is (= #{["full" 1] ["full" 2] ["full" 9]}
               (set (map (juxt :table :bork) in)))))))

(t/deftest test-releases-head
  (let [n (atom 0)

        ref (atom nil)
        _ (println "Constructing seq")

        query-seq (iterator-seq
                   (reify
                     java.util.Iterator
                     (next [_]
                       (println "Generating" (swap! n inc))
                       {:geometry (g/make-point [@n @n])
                        :long-string (str (repeat 100000 @n))})

                     (hasNext [_]
                       (let [gcd (and @ref (nil? (.get @ref)))]
                         ;; generally we make ~32 rows because the
                         ;; iterator is chunked so it will ask for the
                         ;; next 32 items as soon as we want the first
                         ;; and we only get gced once the call to
                         ;; write has got so far.
                         (when (> @n 30) (System/gc))
                         (let [result (and (< @n 100) (not gcd))]
                           result)))))

        out-file (java.io.File/createTempFile "test" ".gpkg")]
    (try
      (reset! ref (java.lang.ref.WeakReference. query-seq))
      (println "Calling write...")
      (sut/write out-file "test"
                 query-seq
                 :schema
                 [["geometry" {:type :point :srid 27700 :accessor :geometry}]
                  ["long-string" {:type :string :accessor :long-string}]]
                 :batch-insert-size 1)
      (t/testing "The sequence head got garbage collected after we started writing"
        (t/is (< @n 90)))
      (finally (io/delete-file out-file true)))))

(t/deftest test-set-layer-extent
  (let [f (.toFile (java.nio.file.Files/createTempFile
                    "test-set-layer-extent" ".gpkg"
                    (into-array java.nio.file.attribute.FileAttribute [])))]
    (try
      ;; Points layer
      (sut/write
       f "test-table-points"
       [{"geometry" (g/make-point 1 2) "id" 1}
        {"geometry" (g/make-point 4 5) "id" 2}
        {"geometry" nil "id" 3}]

       :schema
       {"geometry" {:type :point :srid 27700}
        "id" {:type :integer}})

      (with-open [in (sut/open f :table-name "gpkg_contents")]
        (let [ls (sut/features in)
              row (first ls)]
          (t/is (= 1.0 (get row "min_x")))
          (t/is (= 2.0 (get row "min_y")))
          (t/is (= 4.0 (get row "max_x")))
          (t/is (= 5.0 (get row "max_y")))))

      ;; Polygon layer
      (sut/write
       f "test-table-polygons"
       [{"geometry" (g/make-polygon [[0 0] [0 1] [1 1] [1 0] [0 0]]) "id" 1}
        {"geometry" (g/make-polygon [[1 1] [1 2] [3 2] [3 1] [1 1]]) "id" 2}]

       :schema
       {"geometry" {:type :polygon :srid 27700}
        "id" {:type :integer}})

      (with-open [in (sut/open f :table-name "gpkg_contents")]
        (let [ls (sut/features in)
              row (second ls)]
          (t/is (= 0.0 (get row "min_x")))
          (t/is (= 0.0 (get row "min_y")))
          (t/is (= 3.0 (get row "max_x")))
          (t/is (= 2.0 (get row "max_y")))))

      ;; LineString layer
      (sut/write
       f "test-table"
       [{"geometry" (g/make-line-string [[0 0] [1 1]]) "id" 1}
        {"geometry" (g/make-line-string [[1 1] [1 3]]) "id" 2}]

       :schema
       {"geometry" {:type :line-string :srid 27700}
        "id" {:type :integer}})

      (with-open [in (sut/open f :table-name "gpkg_contents")]
        (let [ls (sut/features in)
              row (nth ls 2)]
          (t/is (= 0.0 (get row "min_x")))
          (t/is (= 0.0 (get row "min_y")))
          (t/is (= 1.0 (get row "max_x")))
          (t/is (= 3.0 (get row "max_y")))))

      (catch Exception e (prn e) (throw e))
      (finally (io/delete-file f)))))

(comment
  (with-open [gpkg (sut/open "/tmp/hnzp-1966294190446547555/Data/oproad_gb.gpkg" :table-name "road_link")
              f (io/writer "/tmp/hnzp-1966294190446547555/test.txt")]
    (doseq [row (sut/features gpkg)]
      (.write f (get row "id"))
      (.write f "\n")))
  )


