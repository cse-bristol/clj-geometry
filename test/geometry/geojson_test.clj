(ns geometry.geojson-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [geometry.core :as g]
            [geometry.geojson :as geojson]))


(io/reader (io/resource "geometry/feature.geojson"))


(deftest should-read
  (testing "Should read a feature"
    (let [g1 (first (with-open [f (io/reader (io/resource "geometry/feature.geojson"))]
                      (geojson/open f)))]
      (is (= (:geometry g1) (g/read-wkt "POLYGON ((100 0, 101 0, 101 1, 100 1, 100 0))")))
      (is (= (get g1 "prop0") "value0"))
      (is (= (get g1 "prop1") {"this" "that"}))))
  
  (testing "Should read a feature collection"
    (let [[g1 g2 g3] (with-open [f (io/reader (io/resource "geometry/feature-collection.geojson"))]
                       (geojson/open f))]
      (is (= (:geometry g1) (g/read-wkt "POINT (102 0.5)")))
      (is (= (get g1 "prop0") "value0"))

      (is (= (:geometry g2) (g/read-wkt "LINESTRING (102 0, 103 1, 104 0, 105 1)")))
      (is (= (get g2 "prop0") "value0"))
      (is (= (get g2 "prop1") 0.0))

      (is (= (:geometry g3) (g/read-wkt "POLYGON ((100 0, 101 0, 101 1, 100 1, 100 0))")))
      (is (= (get g3 "prop0") "value0"))
      (is (= (get g3 "prop1") {"this" "that"})))))
