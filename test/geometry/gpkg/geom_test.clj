(ns geometry.gpkg.geom-test
  (:require [geometry.gpkg.geom :as geom]
            [geometry.core :as g]
            [clojure.test :as t])
  (:import [org.locationtech.jts.geom Geometry GeometryFactory]
           [org.locationtech.jts.io WKBWriter]
           [java.nio ByteBuffer ByteOrder]))

(defn- hex->bytes ^bytes [s]
  (byte-array (for [[a b] (partition 2 s)]
                (unchecked-byte (Integer/parseInt (str a b) 16)))))

(def rdr (geom/reader g/*factory*))

(defn- build-blob
  "Wrap `gm`'s WKB in a GeoPackage header with the given byte order and
   envelope-indicator code (0 none, 1 xy, 2 xyz, 3 xym, 4 xyzm). Envelope
   values are filled with zeros since `decode` only uses them to find the
   WKB offset."
  ^bytes [^Geometry gm srid little-header? env-code]
  (let [wkb    (.write (WKBWriter.) gm)
        n-env  ({0 0, 1 4, 2 6, 3 6, 4 8} env-code)
        flags  (bit-or (if little-header? 0x01 0) (bit-shift-left env-code 1))
        order  (if little-header? ByteOrder/LITTLE_ENDIAN ByteOrder/BIG_ENDIAN)
        header (doto (ByteBuffer/allocate (+ 8 (* 8 n-env)))
                 (.order order)
                 (.put (byte 0x47))
                 (.put (byte 0x50))
                 (.put (byte 0))
                 (.put (byte flags))
                 (.putInt (int srid)))
        _      (dotimes [_ n-env] (.putDouble header 0.0))
        out    (byte-array (+ 8 (* 8 n-env) (alength wkb)))]
    (System/arraycopy (.array header) 0 out 0 (+ 8 (* 8 n-env)))
    (System/arraycopy wkb 0 out (+ 8 (* 8 n-env)) (alength wkb))
    out))

(t/deftest decode-header-variants
  (let [gm (g/make-point 1 2)]
    (doseq [little? [true false]
            env-code [0 1 2 3 4]]
      (t/testing (str "little-header? " little? " env-code " env-code)
        (let [decoded (geom/decode (build-blob gm 27700 little? env-code) rdr)]
          (t/is (= gm decoded))
          (t/is (= 27700 (.getSRID decoded))))))))

(t/deftest decode-bad-magic
  (t/is (thrown? clojure.lang.ExceptionInfo
                 (geom/decode (hex->bytes "0000000000000000") rdr))))

(t/deftest empty-geometry-roundtrip
  (t/testing "an empty (but non-nil) geometry round-trips and stays empty"
    (let [empty-pt (.createPoint (GeometryFactory.))
          blob     (geom/encode empty-pt 27700)
          decoded  (geom/decode blob rdr)]
      (t/is (.isEmpty decoded))
      (t/is (= 27700 (.getSRID decoded)))
      ;; bit 4 of the flags byte signals an empty geometry
      (t/is (bit-test (aget blob 3) 4)))))

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
