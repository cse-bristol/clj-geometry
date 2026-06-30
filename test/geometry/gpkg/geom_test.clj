(ns geometry.gpkg.geom-test
  (:require [geometry.gpkg.geom :as geom]
            [geometry.core :as g]
            [clojure.test :as t]))

(defn- hex->bytes ^bytes [s]
  (byte-array (for [[a b] (partition 2 s)]
                (unchecked-byte (Integer/parseInt (str a b) 16)))))

(def rdr (geom/reader g/*factory*))

(t/deftest decode-geotools-reference
  (t/testing "decodes a real GeoTools-written blob (big-endian header, xy envelope)"
    ;; captured from gpkg/write of (make-point 1 2), srid 27700
    (let [blob (hex->bytes
                (str "4750000200006c34"            ; GP, v0, flags=02, srs=27700
                     "3ff0000000000000"            ; envelope minx = 1.0
                     "3ff0000000000000"            ; envelope maxx = 1.0
                     "4000000000000000"            ; envelope miny = 2.0
                     "4000000000000000"            ; envelope maxy = 2.0
                     "0000000001"                  ; WKB big-endian, type point
                     "3ff0000000000000"            ; x = 1.0
                     "4000000000000000"))          ; y = 2.0
          decoded (geom/decode blob rdr)]
      (t/is (= (g/make-point 1 2) decoded))
      (t/is (= 27700 (.getSRID decoded))))))

(t/deftest roundtrip
  (doseq [gm [(g/make-point 1 2)
              (g/make-line-string [[0 0] [1 1] [2 3]])
              (g/make-polygon [[0 0] [0 1] [1 1] [1 0] [0 0]])
              (g/make-multi-point [(g/make-point 1 2) (g/make-point 3 4)])
              (g/make-multi-polygon [(g/make-polygon [[0 0] [0 1] [1 1] [1 0] [0 0]])])]]
    (t/testing (str (.getGeometryType gm))
      (let [decoded (geom/decode (geom/encode gm 27700) rdr)]
        (t/is (= gm decoded))
        (t/is (= 27700 (.getSRID decoded)))))))

(t/deftest nil-passthrough
  (t/is (nil? (geom/encode nil 27700)))
  (t/is (nil? (geom/decode nil rdr))))
