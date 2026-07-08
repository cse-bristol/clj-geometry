(ns geometry.crs-test
  (:require [geometry.crs :as crs]
            [geometry.core :as g]
            [clojure.test :as t]
            [geometry.core :as geom])
  (:import [org.locationtech.proj4j CoordinateReferenceSystem]))

(t/deftest test-lookup
  (t/testing "known EPSG codes resolve"
    (t/is (instance? CoordinateReferenceSystem (crs/lookup 4326)))
    (t/is (instance? CoordinateReferenceSystem (crs/lookup 27700))))
  (t/testing "an unknown code is nil rather than throwing"
    (t/is (nil? (crs/lookup 999999999)))))

(t/deftest test-srs-row
  (t/testing "a known srid carries an EPSG identity and a real proj4 definition"
    (let [row (crs/srs-row 4326)]
      (t/is (= "EPSG" (:organization row)))
      (t/is (= 4326 (:organization_coordsys_id row)))
      (t/is (not= "undefined" (:definition row)))
      (t/is (string? (:srs_name row)))))

  (t/testing "an unknown srid falls back to an undefined definition"
    (let [row (crs/srs-row 999999999)]
      (t/is (= "EPSG" (:organization row)))
      (t/is (= 999999999 (:organization_coordsys_id row)))
      (t/is (= "undefined" (:definition row)))
      (t/is (= "EPSG:999999999" (:srs_name row)))
      (t/is (nil? (:description row))))))

(t/deftest test-transform-unknown-crs
  (t/is (thrown? clojure.lang.ExceptionInfo (crs/transform 999999999 4326)))
  (t/is (thrown? clojure.lang.ExceptionInfo (crs/transform 4326 999999999))))

(t/deftest test-reproject
  ;; an Ordnance Survey test point in British National Grid (EPSG:27700)
  (let [pt (g/make-point 651409.903 313177.270)
        t  (crs/transform 27700 4326)
        out (crs/reproject pt t 4326)]

    (t/testing "coordinates are reprojected to WGS84 with x=longitude, y=latitude 
                (proj4j uses a Helmert transform, so allow a loose tolerance)"
      (t/is (< (Math/abs (- 1.7179 (.getX out))) 0.01))
      (t/is (< (Math/abs (- 52.6576 (.getY out))) 0.01))

      (t/is (= (g/make-point 452622.19489624136 233605.77503933024)
               (crs/reproject (g/make-point -1.234932 51.998410)
                              (crs/transform 4326 27700) 27700)))
      
      (t/is (= (g/make-point -7.557160831822298 49.766816190946884)
               (crs/reproject (g/make-point 0 1)
                              (crs/transform 27700 4326) 4326))))

    (t/testing "the result SRID is set to the target"
      (t/is (= 4326 (.getSRID out))))

    (t/testing "the source geometry is left untouched"
      (t/is (= 651409.903 (.getX pt)))
      (t/is (= 313177.270 (.getY pt))))

    (t/testing "round-tripping back to 27700 recovers the original point"
      (let [back (crs/reproject out (crs/transform 4326 27700) 27700)]
        (t/is (< (Math/abs (- 651409.903 (.getX back))) 0.01))
        (t/is (< (Math/abs (- 313177.270 (.getY back))) 0.01))))))
