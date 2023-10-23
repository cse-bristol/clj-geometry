(ns geometry.index-test
  (:require [clojure.test :refer [deftest is]]
            [geometry.core :as g]
            [geometry.index :as index])
  (:import [geometry.feature Feature]))

(deftest test-intersecting
  (is (= (->> ["POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"]
              (mapv g/read-wkt)
              (mapv g/normalize))
         (->> (index/intersecting (index/create [(g/read-wkt "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
                                                 (g/read-wkt "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))")])
                                  (g/read-wkt "POINT (5 5)"))
              (mapv g/normalize)))))

(deftest test-intersecting-with-feature
  (is (= (->> ["POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"]
              (mapv g/read-wkt)
              (mapv g/normalize))
         (->> (index/intersecting (index/create [(g/read-wkt "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
                                                 (g/read-wkt "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))")])
                                  (Feature. (g/read-wkt "POINT (5 5)") "table" 27700))
              (mapv g/normalize)))))

(deftest test-intersecting-features-with-feature
  (is (= (->> ["POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"]
              (mapv g/read-wkt)
              (mapv g/normalize))
         (->> (index/intersecting (index/create [(Feature. (g/read-wkt "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))") "table" 27700)
                                                 (Feature. (g/read-wkt "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))") "table" 27700)])
                                  (Feature. (g/read-wkt "POINT (5 5)") "table" 27700))
              (mapv g/normalize)
              (mapv :geometry)))))

(deftest test-touching
  (is (= (->> ["POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"]
              (mapv g/read-wkt)
              (mapv g/normalize))
         (->> (index/touching (index/create [(g/read-wkt "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
                                             (g/read-wkt "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))")])
                              (g/read-wkt "POINT (0 0)"))
              (mapv g/normalize)))))

(deftest test-overlapping
  (is (= (->> ["POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"
               "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))"]
              (mapv g/read-wkt)
              (mapv g/normalize))
         (->> (index/overlapping (index/create [(g/read-wkt "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")
                                                (g/read-wkt "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
                                                (g/read-wkt "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))")])
                                 (g/read-wkt "POLYGON ((5 5, 11 5, 11 11, 5 11, 5 5))"))
              (mapv g/normalize)))))

(deftest test-covered-by
  (is (= (->> ["POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"
               "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))"]
              (mapv g/read-wkt)
              (mapv g/normalize))
         (->> (index/covered-by (index/create [(g/read-wkt "POLYGON ((-1 0, 1 0, 1 1, 0 1, -1 0))")
                                                (g/read-wkt "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
                                                (g/read-wkt "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))")])
                                 (g/read-wkt "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))"))
              (mapv g/normalize)))))
