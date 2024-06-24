(ns geometry.core-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [geometry.core :as geom]
            [geometry.feature :as f]))

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
    (is (= true (geom/multi? (geom/make-collection [(geom/make-point 6 6)]))))))

(deftest polygons-test
  (testing "preserve features"
    (is (=
         [(f/->Feature (geom/make-polygon [[0 0] [0 1] [1 1] [1 0] [0 0]]) "table" 27700)]
         (geom/polygons (f/->Feature (geom/make-polygon [[0 0] [0 1] [1 1] [1 0] [0 0]]) "table" 27700)))))
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

(deftest union-test
  (is (= (norm-all (map geom/read-wkt ["LINESTRING (0 0, 3 3)"
                                       "LINESTRING (3 3, 5 5)"
                                       "LINESTRING (5 5, 10 10)"
                                       "LINESTRING (10 0, 5 5)"
                                       "LINESTRING (5 5, 3 7)"
                                       "LINESTRING (3 7, 0 10)"
                                       "LINESTRING (3 0, 3 3)"
                                       "LINESTRING (3 3, 3 7)"
                                       "LINESTRING (3 7, 3 10)"]))
         (norm-all (geom/line-strings (geom/union (geom/read-wkt "MULTILINESTRING ((0 0, 10 10), (10 0, 0 10))")
                                                  (geom/read-wkt "LINESTRING (3 0, 3 10)"))))
         (norm-all (geom/line-strings (geom/union (geom/make-collection
                                                   [(geom/read-wkt "MULTILINESTRING ((0 0, 10 10), (10 0, 0 10))")
                                                    (geom/read-wkt "LINESTRING (3 0, 3 10)")])))))))

(deftest intersection-test
  (is (= (geom/read-wkt "LINESTRING (0 0, 3 3)")
         (geom/intersection (geom/read-wkt "LINESTRING (0 0, 3 3)")
                            (geom/read-wkt "LINESTRING (0 0, 3 3)"))))

  (is (= (geom/read-wkt "POINT (1.5 1.5)")
         (geom/intersection (geom/read-wkt "LINESTRING (0 0, 3 3)")
                            (geom/read-wkt "LINESTRING (3 0, 0 3)")))))

(deftest overlay-robustness-test
  (testing "Should be able to do overlay operations even when at the limits of floating point accuracy. These tests fail without OverlayNGRobust"
    (let [a (geom/read-wkt "POLYGON ((543574.7 180683.9, 543574.6 180684, 543574.4 180684, 543574.2 180684, 543574.1 180684, 543573 180684.3, 543572.2 180684.3, 543571.6 180684.4, 543569 180684.4, 543559.6 180684.6, 543552.4 180684.7, 543552.4 180685.4, 543550.2 180685.5, 543550.2 180684.8, 543522.1 180685.4, 543522.1 180686.2, 543519.9 180686.2, 543519.9 180685.4, 543500 180685.9, 543491.6 180686, 543491.6 180686.9, 543489.4 180687, 543489.4 180686.1, 543461.4 180686.7, 543461.5 180687.6, 543459.3 180687.7, 543459.3 180686.8, 543430.2 180687.4, 543430.2 180688.3, 543428 180688.3, 543428 180687.4, 543400 180688.1, 543400 180689, 543397.8 180689.1, 543397.8 180688.1, 543369.3 180688.7, 543369.4 180689.7, 543367.2 180689.7, 543367.1 180688.8, 543354.5 180689.1, 543348.9 180683.2, 543312.9 180683.9, 543312.9 180684.9, 543310.7 180684.9, 543310.7 180683.9, 543300 180684.2, 543284 180684.6, 543284 180685.5, 543281.8 180685.5, 543281.8 180684.6, 543254.8 180685.3, 543254.8 180686, 543252.6 180686.1, 543252.6 180685.3, 543241.1 180685.6, 543225.2 180685.9, 543225.2 180686.7, 543223 180686.8, 543223 180686, 543195.9 180686.5, 543195.9 180687.3, 543193.7 180687.4, 543193.7 180686.6, 543166.9 180687.1, 543166.9 180687.9, 543164.7 180687.9, 543164.7 180687.1, 543143.5 180687.7, 543143.5 180695.4, 543043.8 180699.4, 543038.8 180699.6, 543039.8 180747.5, 543039.9 180752.4, 543040.4 180758.5, 543040.9 180765.6, 543044.8 180765.6, 543044.8 180762.6, 543044.9 180761.4, 543045.3 180760.4, 543046 180759.5, 543046.8 180758.9, 543047.8 180758.4, 543069.3 180757.9, 543069.3 180760, 543081.7 180759.7, 543081.9 180766.1, 543088.5 180766, 543088.7 180777.1, 543089 180790.2, 543089.1 180799.6, 543089 180808.3, 543089 180815.1, 543088.9 180821.6, 543090.9 180821.6, 543090.9 180824.5, 543090.5 180827.2, 543094.5 180827.2, 543099.1 180827.1, 543098.8 180824.2, 543098.5 180820, 543098.4 180816.7, 543097.7 180804.1, 543097.2 180790.8, 543096.9 180778.3, 543098.9 180778.3, 543098.7 180765.9, 543099.9 180765.9, 543107.5 180765.9, 543113.2 180765.8, 543113.1 180761, 543112.5 180760.8, 543112.4 180755.1, 543140 180753.9, 543141.9 180754.1, 543143.5 180754.1, 543143.5 180764.1, 543143.5 180765.3, 543143.5 180767.3, 543143.4 180788.9, 543143.4 180794.3, 543143.4 180795.1, 543143.4 180795.7, 543143.4 180801.8, 543143.5 180824.5, 543143.5 180826.6, 543143.5 180832.1, 543143.5 180836.3, 543143.5 180837.2, 543143.5 180842.2, 543143.5 180844.2, 543209 180843.1, 543213.7 180843, 543219.2 180842.9, 543260.8 180841.7, 543261.7 180841.7, 543278.4 180841.2, 543280.3 180840.7, 543281.6 180840.5, 543283.1 180840.3, 543284.4 180840.1, 543286.8 180839.6, 543286.9 180839.6, 543288 180839.2, 543288.3 180839.2, 543291.9 180838.1, 543296.9 180836.4, 543299.8 180835.2, 543301.9 180834.3, 543304 180835.9, 543308.4 180839.4, 543311.5 180841.9, 543311.6 180842, 543312.1 180842.4, 543311.4 180844, 543308.4 180850.4, 543307.4 180850.4, 543300.1 180850.6, 543293.2 180850.7, 543292.7 180850.7, 543288.3 180850.8, 543288.4 180851.6, 543288.4 180852.9, 543288.5 180854.7, 543288.5 180856.1, 543293.1 180856, 543299.3 180855.8, 543306.1 180855.7, 543313.6 180855.5, 543331.3 180855.1, 543334.4 180855.1, 543351.6 180854.6, 543357.9 180854.5, 543365.9 180854.3, 543376.9 180854, 543376.9 180853.1, 543376.9 180850.8, 543368.4 180851.1, 543368.4 180850, 543368.3 180849, 543363.6 180849.1, 543356.6 180849.3, 543355.4 180847, 543355.2 180846.6, 543355.1 180846.4, 543354.8 180845.7, 543354.7 180845.6, 543354.6 180845.4, 543354.4 180845.1, 543353.8 180843.9, 543353.4 180843.3, 543352.6 180842, 543352.2 180841.4, 543351.9 180840.9, 543352.5 180841.3, 543355.7 180839.4, 543360.4 180836.6, 543362.8 180835.1, 543363.8 180835.7, 543365.2 180836.3, 543366.6 180836.9, 543368 180837.5, 543369.4 180838, 543371.4 180838.6, 543372.2 180838.8, 543373 180839, 543374.5 180839.4, 543375.3 180839.6, 543376 180839.7, 543376.8 180839.8, 543377.6 180839.9, 543378.8 180839.9, 543379.3 180839.8, 543384.5 180839.6, 543385.1 180839.6, 543387.6 180839.5, 543395.1 180840.6, 543421.2 180840.2, 543440.8 180839.8, 543448.1 180838.9, 543455 180838.2, 543458.4 180836.2, 543459.6 180835.5, 543465.2 180832, 543471.7 180827, 543480.8 180819.9, 543489.7 180812.8, 543493.6 180809.4, 543497.3 180806.4, 543500 180804.1, 543538.6 180770.8, 543543.2 180761.5, 543569.8 180739.8, 543587.6 180732.1, 543588.2 180731.9, 543599.6 180727.4, 543601.7 180726.5, 543606 180724.6, 543610.9 180722.5, 543615.9 180720.5, 543620.7 180718.8, 543626 180717.1, 543629.3 180716.1, 543631 180715.7, 543635 180714.6, 543636.4 180714.3, 543638.9 180713.7, 543640.9 180713.2, 543641.8 180713, 543645.2 180712.3, 543645.5 180712.3, 543648.2 180711.8, 543651 180711.3, 543656.5 180710.5, 543657.2 180710.4, 543663 180709.8, 543666 180709.5, 543668.8 180709.3, 543671.1 180709.1, 543671.5 180709.1, 543675.1 180709, 543676 180708.9, 543677 180708.9, 543680.1 180708.8, 543680.6 180708.8, 543680.7 180708.8, 543680.9 180708.4, 543681.1 180708.8, 543681.2 180708.8, 543684.8 180708.7, 543686.7 180708.7, 543689.2 180708.7, 543696.7 180708.7, 543697.7 180708.7, 543714.6 180709, 543717.2 180709, 543724 180709.1, 543726.6 180709.1, 543728.8 180709.2, 543730.4 180709.2, 543730.8 180709.2, 543732.7 180709.3, 543732.9 180709.3, 543734.9 180709.4, 543736.4 180709.4, 543738.8 180709.6, 543739.3 180709.6, 543743.5 180709.9, 543747.1 180710.2, 543748.5 180710.4, 543748.8 180710.4, 543749.4 180710.5, 543750.8 180710.7, 543753 180711, 543754 180711.2, 543756.1 180711.5, 543758.9 180712, 543759.8 180712.2, 543763.8 180713, 543766.5 180713.6, 543767 180713.7, 543768.6 180714.1, 543772.6 180715.2, 543777.3 180716.6, 543781.9 180718.1, 543786.4 180719.8, 543788.6 180720.7, 543793 180722.5, 543793.1 180722.5, 543793.5 180722.7, 543797.3 180724.5, 543800.9 180726.3, 543802.5 180727.2, 543802.6 180727.1, 543806.2 180723.3, 543807.1 180722.1, 543807.9 180721, 543808.6 180719.9, 543809.2 180718.9, 543809.4 180718.5, 543811.1 180714.6, 543811.3 180714.1, 543811.8 180712.9, 543814.7 180706.7, 543818.5 180695.3, 543820.2 180691.7, 543824.1 180682.3, 543826.9 180674.9, 543828.9 180667.7, 543829.8 180664, 543831 180659.3, 543831.1 180658.8, 543832.6 180652.6, 543833.2 180649.9, 543835.2 180641, 543836.9 180632.1, 543837.7 180627.9, 543838.4 180624.1, 543838.6 180623.1, 543839.1 180619.7, 543836.6110294118 180619.7, 543823.8 180633, 543822.4663793104 180633.03362068965, 543822.2 180633.3, 543785.7 180634.2, 543783.7 180634.3, 543743.2 180635.2, 543708.5 180636, 543670.1 180636.9, 543670.4 180636.7, 543665.2 180639.30000000005, 543665.2 180639.4, 543663.2 180640.4, 543663.2 180640.30000000005, 543660.4 180641.7, 543642.3 180650.7, 543621.6 180661, 543599.8 180671.9, 543586.3 180678.7, 543582.9 180680.40000000002, 543583 180680.6, 543581.1 180681.6, 543581 180681.4, 543582.899999999 180680.40000000058, 543579.3 180682.2, 543576.5 180683.5, 543576.3 180683.6, 543576.2 180683.6, 543576 180683.6, 543575.8 180683.7, 543574.8 180683.9, 543574.7 180683.9), (543610.5 180666.7, 543608.5 180667.7, 543608.4 180667.6, 543610.4 180666.6, 543610.5 180666.7), (543637.8 180653.1, 543635.8 180654.1, 543635.7 180654, 543637.7 180653, 543637.8 180653.1))")
          b (geom/read-wkt "POLYGON ((543491.6 180686.9, 543489.4 180687, 543489.4 180686.1, 543461.4 180686.7, 543461.5 180687.6, 543459.3 180687.7, 543459.3 180686.8, 543430.2 180687.4, 543430.2 180688.3, 543428 180688.3, 543428 180687.4, 543400 180688.1, 543400 180689, 543397.8 180689.1, 543397.8 180688.1, 543369.3 180688.7, 543369.4 180689.7, 543367.2 180689.7, 543367.1 180688.8, 543354.5 180689.1, 543348.9 180683.2, 543312.9 180683.9, 543312.9 180684.9, 543310.7 180684.9, 543310.7 180683.9, 543300 180684.2, 543284 180684.6, 543284 180685.5, 543281.8 180685.5, 543281.8 180684.6, 543254.8 180685.3, 543254.8 180686, 543252.6 180686.1, 543252.6 180685.3, 543241.1 180685.6, 543225.2 180685.9, 543225.2 180686.7, 543223 180686.8, 543223 180686, 543195.9 180686.5, 543195.9 180687.3, 543193.7 180687.4, 543193.7 180686.6, 543166.9 180687.1, 543166.9 180687.9, 543164.7 180687.9, 543164.7 180687.1, 543143.5 180687.7, 543143.5 180695.4, 543143.5 180696.4, 543143.5 180706.3, 543143.5 180709.8, 543143.5 180712.7, 543143.6 180725.3, 543143.6 180725.4, 543143.6 180731.3, 543143.6 180741.7, 543143.5 180743.5, 543143.5 180745.5, 543143.5 180749.2, 543143.5 180749.4, 543143.5 180751.4, 543143.5 180754.1, 543143.5 180764.1, 543143.5 180765.3, 543143.5 180767.3, 543143.4 180788.9, 543143.4 180794.3, 543143.4 180795.1, 543143.4 180795.7, 543143.4 180801.8, 543143.5 180824.5, 543143.5 180826.6, 543143.5 180832.1, 543143.5 180836.3, 543143.5 180837.2, 543143.5 180842.2, 543143.5 180844.2, 543209 180843.1, 543213.7 180843, 543219.2 180842.9, 543260.8 180841.7, 543278.4 180841.2, 543280.3 180840.7, 543281.6 180840.5, 543283.1 180840.3, 543284.4 180840.1, 543286.8 180839.6, 543286.9 180839.6, 543288 180839.2, 543288.3 180839.2, 543291.9 180838.1, 543296.9 180836.4, 543299.8 180835.2, 543301.9 180834.3, 543304 180835.9, 543312.1 180842.4, 543325.1 180833.5, 543328.4 180832.6, 543336.6 180832.5, 543339.9 180833.3, 543352.5 180841.3, 543360.4 180836.6, 543362.8 180835.1, 543363.8 180835.7, 543365.2 180836.3, 543366.6 180836.9, 543368 180837.5, 543369.4 180838, 543371.4 180838.6, 543372.2 180838.8, 543373 180839, 543374.5 180839.4, 543375.3 180839.6, 543376 180839.7, 543376.8 180839.8, 543377.6 180839.9, 543378.8 180839.9, 543379.3 180839.8, 543384.5 180839.6, 543385.1 180839.6, 543387.6 180839.5, 543395.1 180840.6, 543421.2 180840.2, 543440.8 180839.8, 543448.1 180838.9, 543455 180838.2, 543458.4 180836.2, 543459.6 180835.5, 543465.2 180832, 543471.7 180827, 543480.8 180819.9, 543489.7 180812.8, 543493.6 180809.4, 543497.3 180806.4, 543500 180804.1, 543538.6 180770.8, 543543.2 180761.5, 543569.8 180739.8, 543587.6 180732.1, 543588.2 180731.9, 543599.6 180727.4, 543601.7 180726.5, 543606 180724.6, 543610.9 180722.5, 543615.9 180720.5, 543620.7 180718.8, 543626 180717.1, 543629.3 180716.1, 543631 180715.7, 543635 180714.6, 543636.4 180714.3, 543638.9 180713.7, 543640.9 180713.2, 543641.8 180713, 543645.2 180712.3, 543645.5 180712.3, 543648.2 180711.8, 543651 180711.3, 543656.5 180710.5, 543657.2 180710.4, 543663 180709.8, 543666 180709.5, 543668.8 180709.3, 543671.1 180709.1, 543671.5 180709.1, 543675.1 180709, 543676 180708.9, 543677 180708.9, 543680.1 180708.8, 543680.6 180708.8, 543681.1 180708.8, 543684.8 180708.7, 543686.7 180708.7, 543689.2 180708.7, 543696.7 180708.7, 543697.7 180708.7, 543714.6 180709, 543717.2 180709, 543726.6 180709.1, 543728.8 180709.2, 543730.4 180709.2, 543730.8 180709.2, 543732.7 180709.3, 543732.9 180709.3, 543734.9 180709.4, 543736.4 180709.4, 543738.8 180709.6, 543739.3 180709.6, 543743.5 180709.9, 543747.1 180710.2, 543748.5 180710.4, 543748.8 180710.4, 543749.4 180710.5, 543750.8 180710.7, 543753 180711, 543754 180711.2, 543756.1 180711.5, 543758.9 180712, 543759.8 180712.2, 543763.8 180713, 543766.5 180713.6, 543767 180713.7, 543768.6 180714.1, 543772.6 180715.2, 543777.3 180716.6, 543781.9 180718.1, 543786.4 180719.8, 543788.6 180720.7, 543793 180722.5, 543793.1 180722.5, 543793.5 180722.7, 543797.3 180724.5, 543800.9 180726.3, 543802.5 180727.2, 543802.6 180727.1, 543806.2 180723.3, 543807.1 180722.1, 543807.9 180721, 543808.6 180719.9, 543809.2 180718.9, 543809.4 180718.5, 543811.1 180714.6, 543811.3 180714.1, 543811.8 180712.9, 543814.7 180706.7, 543818.5 180695.3, 543820.2 180691.7, 543824.1 180682.3, 543826.9 180674.9, 543828.9 180667.7, 543829.8 180664, 543831 180659.3, 543831.1 180658.8, 543832.6 180652.6, 543833.2 180649.9, 543835.2 180641, 543836.9 180632.1, 543838.4 180624.1, 543838.6 180623.1, 543839.1 180619.7, 543835.3 180619.7, 543822.2 180633.3, 543785.7 180634.2, 543783.7 180634.3, 543743.2 180635.2, 543708.5 180636, 543668.7 180636.9, 543664.9 180638.8, 543665.2 180639.4, 543663.2 180640.4, 543662.9 180639.8, 543637.4 180652.4, 543637.8 180653.1, 543635.8 180654.1, 543635.5 180653.4, 543610.1 180666, 543610.5 180666.7, 543608.5 180667.7, 543608.1 180667, 543584.5 180678.9, 543582.6 180679.9, 543583 180680.6, 543581.1 180681.6, 543580.7 180680.9, 543577.6 180682.6, 543574.8 180683.8, 543574.6 180684, 543574.4 180684, 543574.2 180684, 543574.1 180684, 543573 180684.3, 543572.2 180684.3, 543571.6 180684.4, 543569 180684.4, 543559.6 180684.6, 543552.4 180684.7, 543552.4 180685.4, 543550.2 180685.5, 543550.2 180684.8, 543522.1 180685.4, 543522.1 180686.2, 543519.9 180686.2, 543519.9 180685.4, 543500 180685.9, 543491.6 180686, 543491.6 180686.9))")
          c (geom/read-wkt "POLYGON ((543039.8 180747.5, 543039.9 180752.4, 543040.4 180758.5, 543040.9 180765.6, 543044.8 180765.6, 543044.8 180762.6, 543044.9 180761.4, 543045.3 180760.4, 543046 180759.5, 543046.8 180758.9, 543047.8 180758.4, 543069.3 180757.9, 543069.3 180760, 543081.7 180759.7, 543081.9 180766.1, 543088.5 180766, 543088.7 180777.1, 543089 180790.2, 543089.1 180799.6, 543089 180808.3, 543089 180815.1, 543088.9 180821.6, 543090.9 180821.6, 543090.9 180824.5, 543090.5 180827.2, 543099.1 180827.1, 543098.8 180824.2, 543098.5 180820, 543098.4 180816.7, 543097.7 180804.1, 543097.2 180790.8, 543096.9 180778.3, 543098.9 180778.3, 543098.7 180765.9, 543099.9 180765.9, 543107.5 180765.9, 543113.2 180765.8, 543113.1 180761, 543112.5 180760.8, 543112.4 180755.1, 543140 180753.9, 543141.9 180754.1, 543143.5 180754.1, 543143.5 180751.4, 543143.5 180749.4, 543143.5 180749.2, 543143.5 180745.5, 543143.5 180743.5, 543143.6 180741.7, 543143.6 180731.3, 543143.6 180725.4, 543143.6 180725.3, 543143.5 180712.7, 543143.5 180709.8, 543143.5 180706.3, 543143.5 180696.4, 543143.5 180695.4, 543038.8 180699.6, 543039.8 180747.5))")
          d (geom/read-wkt "POLYGON ((543368.4 180851.1, 543368.4 180850, 543368.3 180849, 543363.6 180849.1, 543356.6 180849.3, 543354.7 180845.6, 543352.5 180841.3, 543339.9 180833.3, 543336.6 180832.5, 543328.4 180832.6, 543325.1 180833.5, 543312.1 180842.4, 543311.4 180844, 543308.4 180850.4, 543300.1 180850.6, 543293.2 180850.7, 543288.3 180850.8, 543288.4 180851.6, 543288.4 180852.9, 543288.5 180854.7, 543288.5 180856.1, 543299.3 180855.8, 543313.6 180855.5, 543331.3 180855.1, 543334.4 180855.1, 543351.6 180854.6, 543365.9 180854.3, 543376.9 180854, 543376.9 180853.1, 543376.9 180850.8, 543368.4 180851.1))")]
      (-> a
          (geom/difference b)
          (geom/polygons-of)
          (geom/make-multi-polygon)
          (geom/difference c)
          (geom/polygons-of)
          (geom/make-multi-polygon)
          (geom/difference d))
      (is true))
    (let [a (geom/read-wkt "POLYGON ((117.25669107621917 31.88087184799008, 117.25520887212771 31.880802859126742, 117.25442734633403 31.880814357274257, 117.25389285455559 31.881094145083686, 117.2537311595638 31.882109805947422, 117.25282387099872 31.88289549810332, 117.25232081991314 31.883106292408414, 117.25170997216635 31.88297215063427, 117.25181327729999 31.882174961164015, 117.25170098911124 31.881339437799713, 117.25098234470326 31.88026627729769, 117.25034005626362 31.880231782643566, 117.24997624253209 31.880511572239733, 117.25058709027887 31.881523406903888, 117.25026370029528 31.88170737563706, 117.2498864119811 31.881761033114348, 117.24945971686385 31.88144292046647, 117.24871861481812 31.881032821801625, 117.24814369929173 31.88112097400662, 117.24808081790603 31.881833853925908, 117.24808530943359 31.882669372761757, 117.24629768146873 31.88076836467552, 117.24537242679345 31.879837009562078, 117.24488285029051 31.87930808761019, 117.24463132474771 31.878821323394163, 117.24814369929173 31.878487869243305, 117.24807183485093 31.8753909035069, 117.24807183485093 31.875298912818067, 117.24805386874074 31.868740504315234, 117.24807632637848 31.863159161314687, 117.24804937721319 31.85753914012856, 117.25040293764933 31.857642649784946, 117.25284632863647 31.857945510698194, 117.25647997442432 31.858953761942487, 117.25741870368226 31.85924511764295, 117.2574501443751 31.859904498163512, 117.25673149996712 31.880664881243572, 117.25669107621917 31.88087184799008))")
          b (geom/read-wkt "POLYGON ((117.25733381625201 31.863265014673992, 117.25105232709294 31.86347350418914, 117.24835452233238 31.87129622640433, 117.2489365393494 31.870178694603474, 117.25698972630828 31.873205172792094, 117.25694481874807 31.87450247385353, 117.25694481874807 31.874502473853532, 117.25733381625201 31.863265014673992))")]
      (geom/intersection a b)
      (is true))))

(deftest buffer-test
  (let [p (geom/read-wkt (slurp (io/resource "geometry/polygon1.wkt")))]
    (is (geom/valid? (geom/buffer (geom/buffer p -30 2 :square :mitre 2) 45 2 :square :mitre 2))))
  (let [p (geom/read-wkt (slurp (io/resource "geometry/polygon2.wkt")))]
    (is (geom/valid? (geom/buffer (geom/buffer p -10 2 :square :mitre 2) 15 2 :square :mitre 2)))))

(deftest line-merge-test
  (is (= [(geom/read-wkt "LINESTRING (0 0, 3 3, 5 5, 3 1)")]
         (geom/line-merge (geom/make-collection [(geom/read-wkt "LINESTRING (0 0, 3 3)")
                                                 (geom/read-wkt "LINESTRING (3 3, 5 5)")
                                                 (geom/read-wkt "LINESTRING (5 5, 3 1)")]))))
  (is (= [(geom/read-wkt "LINESTRING (0 0, 3 3)") (geom/read-wkt "LINESTRING (0 5, 5 5)")]
         (geom/line-merge (geom/make-collection [(geom/read-wkt "LINESTRING (0 0, 3 3)")
                                                 (geom/read-wkt "LINESTRING (0 5, 5 5)")]))))
  (is (= [(geom/read-wkt "LINESTRING (0 0, 0 1, 0 5)")
          (geom/read-wkt "LINESTRING (0 10, 10 10)")
          (geom/read-wkt "LINESTRING (1 1, 1 5, 8 7)")]
         (geom/line-merge (geom/make-collection [(geom/read-wkt "LINESTRING (0 0, 0 1)")
                                                 (geom/read-wkt "LINESTRING (0 1, 0 5)")
                                                 (geom/read-wkt "LINESTRING (1 1, 1 5)")
                                                 (geom/read-wkt "LINESTRING (1 5, 8 7)")
                                                 (geom/read-wkt "LINESTRING (0 10, 10 10)")])))))

(deftest cut-line-test
  (testing "no intersections"
    (is
     (= [(geom/read-wkt "LINESTRING (0 0, 10 0, 10 10, 0 10, 0 0)")]
        (geom/cut-line (geom/make-line-string [[0 0] [0 10] [10 10] [10 0] [0 0]])
                       (geom/make-line-string [[-1 -1] [10 -1]])))))
  
  (testing "no intersections linear ring"
    (is
     (= [(geom/read-wkt "LINEARRING (0 0, 0 10, 10 10, 10 0, 0 0)")]
        (geom/cut-line (geom/boundary-of (geom/make-polygon [[0 0] [0 10] [10 10] [10 0] [0 0]]))
                       (geom/make-line-string [[-1 -1] [10 -1]])))))

  (testing "one intersection in linestring"
    (is
     (= [(geom/read-wkt "LINESTRING (0 2, 0 0)")
         (geom/read-wkt "LINESTRING (1 0, 10 0, 10 10, 0 10, 0 2)")
         (geom/read-wkt "LINESTRING (0 0, 1 0)")]
        (geom/cut-line (geom/make-line-string [[0 0] [0 10] [10 10] [10 0] [0 0]])
                       (geom/make-line-string [[-1 -1] [1 5] [1 -1]])))))

  (testing "one intersection in linear ring"
    (is
     (= [(geom/read-wkt "LINESTRING (1 0, 0 0, 0 2)")
         (geom/read-wkt "LINESTRING (1 0, 10 0, 10 10, 0 10, 0 2)")]
        (geom/cut-line (geom/boundary-of (geom/make-polygon [[0 0] [0 10] [10 10] [10 0] [0 0]]))
                       (geom/make-line-string [[-1 -1] [1 5] [1 -1]])))))

  (testing "two intersections in linestring"
    (is
     (= [(geom/read-wkt "LINESTRING (0 2, 0 0)")
         (geom/read-wkt "LINESTRING (3 0, 10 0, 10 10, 0 10, 0 2)")
         (geom/read-wkt "LINESTRING (1.3333333333333357 0, 3 0)")
         (geom/read-wkt "LINESTRING (1 0, 1.3333333333333357 0)")
         (geom/read-wkt "LINESTRING (0 0, 1 0)")]
        (geom/cut-line (geom/make-line-string [[0 0] [0 10] [10 10] [10 0] [0 0]])
                       (geom/make-line-string [[-1 -1] [1 5] [1 -1] [3 5] [3 -1]])))))

  (testing "two intersections in linear ring, one goes round the other"
    (is 
     (= [(geom/read-wkt "LINESTRING (1 0, 0 0, 0 2)")
         (geom/read-wkt "LINESTRING (0 6, 0 2)")
         (geom/read-wkt "LINESTRING (1.2857142857142847 0, 10 0, 10 10, 0 10, 0 6)")
         (geom/read-wkt "LINESTRING (1 0, 1.2857142857142847 0)")]
        (geom/cut-line (geom/boundary-of (geom/make-polygon [[0 0] [0 10] [10 10] [10 0] [0 0]]))
                       (geom/make-line-string [[-1 -1] [1 5] [1 -1] [3 6] [-1 6]]))))))
