(ns geometry.gpkg-test
  (:require [geometry.gpkg :as sut]
            [clojure.test :as t]
            [clojure.java.io :as io]
            [geometry.core :as g])
  (:import [org.geotools.geometry.jts Geometries]))

(t/deftest test-write-read
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
      
      (with-open [in (sut/open f "test-table")]
        (t/is (= [{:geometry (g/make-point 1 2) "id" 1 :table "test-table" :crs "EPSG:27700"}
                  {:geometry (g/make-point 4 5) "id" 2 :table "test-table" :crs "EPSG:27700"}]

                 ;; we map into {} to strip off the feature type
                 ;; since we want to do a simple comparison here
                 (vec (map #(into {} %) in)))))
            
      (finally (io/delete-file f)))))

