(ns geometry.index-test
  (:require [clojure.test :refer [deftest is testing]]
            [geometry.core :as g]
            [geometry.feature :as f]
            [geometry.index :as index])
  (:import [geometry.feature Feature]))

(deftest test-create
  (let [idx
        (index/create [(g/read-wkt "POLYGON EMPTY")
                       nil
                       (g/read-wkt "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")])]
    (is (= (vec (index/entries idx)) 
           [(g/read-wkt "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")]))))

(deftest test-intersecting
  (is (= (->> ["POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"]
              (mapv g/read-wkt)
              (mapv g/normalize))
         (->> (index/intersecting (index/create [(g/read-wkt "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
                                                 (g/read-wkt "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))")])
                                  (g/read-wkt "POINT (5 5)"))
              (mapv g/normalize))))
  (testing "Should return empty vector if query is an empty geom"
    (is (= []
           (index/intersecting (index/create [(g/read-wkt "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
                                              (g/read-wkt "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))")])
                               (g/read-wkt "POLYGON EMPTY"))))))

(deftest test-centroid-intersecting
  (is (= (->> ["POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"]
              (mapv g/read-wkt)
              (mapv g/normalize))
         (->> (index/centroid-intersecting (index/create [(g/read-wkt "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
                                                          (g/read-wkt "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))")])
                                           (g/read-wkt "POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))"))
              (mapv g/normalize))))
  (is (= []
         (->> (index/centroid-intersecting (index/create [(g/read-wkt "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
                                                          (g/read-wkt "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))")])
                                           (g/read-wkt "POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))"))
              (mapv g/normalize)))))

(deftest test-intersecting-with-feature
  (is (= (->> ["POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"]
              (mapv g/read-wkt)
              (mapv g/normalize))
         (->> (index/intersecting (index/create [(g/read-wkt "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
                                                 (g/read-wkt "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))")])
                                  (f/->Feature (g/read-wkt "POINT (5 5)") "table" 27700))
              (mapv g/normalize)))))

(deftest test-intersecting-features-with-feature
  (is (= [(f/map->Feature
           {:geometry "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))"
            :table    "table1"
            :crs      27700})]
         (->> (index/intersecting (index/create [(f/->Feature (g/read-wkt "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))") "table1" 27700)
                                                 (f/->Feature (g/read-wkt "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))") "table2" 27700)])
                                  (f/->Feature (g/read-wkt "POINT (5 5)") "table3" 27700))
              (mapv g/normalize)
              (mapv #(update % :geometry g/write-wkt))))))

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

(deftest test-neighbours
  (let [idx (index/create [(g/read-wkt "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
                           (g/read-wkt "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))")
                           (g/read-wkt "POLYGON ((1010 1010, 1020 1010, 1020 1020, 1010 1020, 1010 1010))")])]
    (is (= (->> ["POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"
                 "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))"]
                (mapv g/read-wkt)
                (mapv g/normalize))
           (->> (index/neighbours idx (g/read-wkt "POINT (5 5)") 100 10)
                (mapv g/normalize))))
    
    (is (= (->> ["POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"]
                (mapv g/read-wkt)
                (mapv g/normalize))
           (->> (index/neighbours idx (g/read-wkt "POINT (5 5)") 1 10)
                (mapv g/normalize))))
    
    (is (= (->> ["POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"]
                (mapv g/read-wkt)
                (mapv g/normalize))
           (->> (index/neighbours idx (g/read-wkt "POINT (5 5)") 100 1)
                (mapv g/normalize))))
    
    (testing "Should return empty vector if query is an empty geom"
      (is (= []
             (index/neighbours idx (g/read-wkt "POLYGON EMPTY") 100 1))))))

(deftest test-query
  (let [idx (index/create [(g/read-wkt "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
                           (g/read-wkt "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))")])]
    (is (= (->> ["POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"]
                (mapv g/read-wkt)
                (mapv g/normalize))
           (->> (index/query idx (g/read-wkt "POINT (5 5)"))
                (mapv g/normalize))))

    (testing "Should return empty vector if query is an empty geom"
      (is (= [] (index/query idx (g/read-wkt "POLYGON EMPTY")))))

    (testing "Should work when a radius is given"
      (is (= (->> ["POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"
                   "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))"]
                  (mapv g/read-wkt)
                  (mapv g/normalize))
             (->> (index/query idx (g/read-wkt "POINT (5 5)") (inc (Math/sqrt 50)))
                  (mapv g/normalize)))))

    (testing "Should return empty vector if query is an empty geom and a radius is given"
      (is (= [] (index/query idx (g/read-wkt "POLYGON EMPTY") 100))))))
(deftest test-within-distance
  (let [;; the long line's bounding box is nearer the query than the small square,
        ;; but the line itself is further away
        line   (g/read-wkt "LINESTRING (0 100, 100 0)")
        square (g/read-wkt "POLYGON ((25 25, 27 25, 27 27, 25 27, 25 25))")
        far    (g/read-wkt "POINT (500 500)")
        idx    (index/create [line square far])
        q      (g/read-wkt "POINT (20 20)")]
    (is (= [square line] (index/within-distance idx q 100)))
    (is (= [square] (index/within-distance idx q 20)))
    (is (= [] (index/within-distance idx q 1)))
    (is (= [] (index/within-distance idx (g/read-wkt "POINT EMPTY") 100)))
    (testing "neighbours with n = 1 picks the line, by its bounding box"
      (is (= [line] (index/neighbours idx q 100 1))))))

(deftest test-nearest
  (let [a   (f/map->Feature {:geometry (g/read-wkt "POINT (0 10)") :id 2})
        b   (f/map->Feature {:geometry (g/read-wkt "POINT (10 0)") :id 1})
        c   (f/map->Feature {:geometry (g/read-wkt "POINT (20 20)") :id 0})
        idx (index/create [a b c])
        q   (g/read-wkt "POINT (0 0)")]
    (is (#{a b} (index/nearest idx q 100)))
    (is (= b (index/nearest idx q 100 :id)) "ties go to the least key")
    (is (nil? (index/nearest idx q 5)))
    (is (nil? (index/nearest idx q 5 :id)))
    (is (nil? (index/nearest idx (g/read-wkt "POINT EMPTY") 100 :id)))))
