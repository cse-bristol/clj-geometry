(ns geometry.grid-test
  (:require [clojure.test :refer [deftest is]]
            [geometry.core :as g]
            [geometry.grid :as grid]))

(deftest test-grid
  (is (= (->> (grid/make-grid (g/read-wkt "POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))") 100)
              (map g/normalize)
              (set))
         #{(g/read-wkt "POLYGON ((-100 -100, -100 0, 0 0, 0 -100, -100 -100))")
           (g/read-wkt "POLYGON ((0 -100, 0 0, 100 0, 100 -100, 0 -100))")
           (g/read-wkt "POLYGON ((100 -100, 100 0, 200 0, 200 -100, 100 -100))")
           (g/read-wkt "POLYGON ((-100 0, -100 100, 0 100, 0 0, -100 0))")
           (g/read-wkt "POLYGON ((0 0, 0 100, 100 100, 100 0, 0 0))")
           (g/read-wkt "POLYGON ((100 0, 100 100, 200 100, 200 0, 100 0))")
           (g/read-wkt "POLYGON ((-100 100, -100 200, 0 200, 0 100, -100 100))")
           (g/read-wkt "POLYGON ((0 100, 0 200, 100 200, 100 100, 0 100))")
           (g/read-wkt "POLYGON ((100 100, 100 200, 200 200, 200 100, 100 100))")})))
