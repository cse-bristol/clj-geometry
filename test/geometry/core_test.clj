(ns geometry.core-test 
  (:require [clojure.test :refer [deftest is testing]]
            [geometry.core :as geom]))

(defn- norm-all [gs]
  (map geom/normalize gs))

(deftest types-test
  (testing "test no spelling mistakes in the type test functions"
    (is (= true (geom/point? (geom/make-point 1 1))))
    (is (= true (geom/single? (geom/make-point 1 1))))

    (is (= true (geom/multi-point? (geom/make-multi-point [[0 0] [1 1]]))))
    (is (= true (geom/multi? (geom/make-multi-point [[0 0] [1 1]]))))

    (is (= true (geom/line-string? (geom/make-line-string [[0 0] [1 1]]))))
    (is (= true (geom/single? (geom/make-line-string [[0 0] [1 1]]))))

    (is (= true (geom/multi-line-string? (geom/make-multi-line-string [[[0 0] [1 1]] [[2 2] [4 4]]]))))
    (is (= true (geom/multi? (geom/make-multi-line-string [[[0 0] [1 1]] [[2 2] [4 4]]]))))

    (is (= true (geom/polygon? (geom/make-polygon [[0 0] [0 1] [1 1] [1 0] [0 0]]))))
    (is (= true (geom/single? (geom/make-polygon [[0 0] [0 1] [1 1] [1 0] [0 0]]))))

    (is (= true (geom/multi-polygon? (geom/make-multi-polygon [[[0 0] [0 1] [1 1] [1 0] [0 0]]]))))
    (is (= true (geom/multi? (geom/make-multi-polygon [[[0 0] [0 1] [1 1] [1 0] [0 0]]]))))
    
    (is (= true (geom/collection? (geom/make-collection [(geom/make-point 6 6)]))))
    (is (= true (geom/multi? (geom/make-collection [(geom/make-point 6 6)]))))
    ))

(deftest polygons-test
  (testing "filters out non-polygons from geometry collections"
    (is (=
         [(geom/make-polygon [[0 0] [0 1] [1 1] [1 0] [0 0]])]
         (geom/polygons (geom/make-collection [(geom/make-point 5 5)
                                               (geom/make-polygon [[0 0] [0 1] [1 1] [1 0] [0 0]])])))))
  (testing "filters out non-polygons from and finds polygons in nested geometry collections"
    (is (=
         [(geom/make-polygon [[0 0] [0 1] [1 1] [1 0] [0 0]])
          (geom/make-polygon [[2 2] [2 3] [3 3] [3 2] [2 2]])]
         (geom/polygons (geom/make-collection [(geom/make-point 5 5)
                                               (geom/make-polygon [[0 0] [0 1] [1 1] [1 0] [0 0]])
                                               (geom/make-collection [(geom/make-point 6 6)
                                                                      (geom/make-polygon [[2 2] [2 3] [3 3] [3 2] [2 2]])])]))))))
(deftest make-valid-test
  (testing "should work on linestrings (the buffer 0 trick turns them into empty geometries)"
    (is (= (geom/make-line-string [[0 0] [10 10] [2 2]])
           (geom/make-valid (geom/make-line-string [[0 0] [10 10] [2 2]]))))))

(deftest holes-test
  (testing "should get the holes from a polygon"
    (is (= [(geom/make-polygon [[5 5] [5 6] [6 6] [6 5] [5 5]])]
           (geom/holes-of (geom/make-polygon [[0 0] [0 10] [10 10] [10 0] [0 0]]
                                             [[[5 5] [5 6] [6 6] [6 5] [5 5]]])))))
  (testing "should get the holes from a multipolygon"
    (is (= [(geom/make-polygon [[5 5] [5 6] [6 6] [6 5] [5 5]])]
           (geom/holes-of (geom/make-multi-polygon [(geom/make-polygon [[0 0] [0 10] [10 10] [10 0] [0 0]]
                                                                       [[[5 5] [5 6] [6 6] [6 5] [5 5]]])]))))))

(deftest cut-polygon-test
  (testing "should cut a polygon"
    (is (= (norm-all [(geom/make-polygon [[0 0] [0 5] [10 5] [10 0] [0 0]])
                      (geom/make-polygon [[0 5] [0 10] [10 10] [10 5] [0 5]])])
           (norm-all (geom/cut-polygon (geom/make-polygon [[0 0] [0 10] [10 10] [10 0] [0 0]])
                                       [(geom/make-line-string [[0 5] [10 5]])]))))))
