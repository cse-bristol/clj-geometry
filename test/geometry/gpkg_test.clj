(ns geometry.gpkg-test
  (:require [geometry.gpkg :as sut]
            [clojure.test :as t]
            [clojure.java.io :as io]
            [geometry.core :as g])
  (:import [org.geotools.geometry.jts Geometries]))

(t/deftest test-spatial-roundtrip
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
      
      ;; read them back and compare
      
      (with-open [in (sut/open f :table-name "test-table")]
        (t/is (= [{:geometry (g/make-point 1 2) "id" 1 :table "test-table" :crs "EPSG:27700"}
                  {:geometry (g/make-point 4 5) "id" 2 :table "test-table" :crs "EPSG:27700"}]

                 ;; we map into {} to strip off the feature type
                 ;; since we want to do a simple comparison here
                 (vec (map #(into {} %) in)))))
      
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
                 (vec (map #(into {} %) in)))))
            
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
                 (vec (map #(into {} %) in)))))
            
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
            
      (catch Exception e (prn e) (throw e))
      (finally (io/delete-file f)))))
