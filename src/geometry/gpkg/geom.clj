(ns geometry.gpkg.geom
  "Codec for the GeoPackage geometry blob format (the \"GP\" binary
   envelope around standard WKB).

   See http://www.geopackage.org/spec/#gpb_format

   A blob is a small header:

     byte[2] magic   = 0x47 0x50 (\"GP\")
     byte    version = 0
     byte    flags
     int32   srs_id
     double[] envelope   (0, 4, 6 or 8 doubles depending on the flags)

   followed by the geometry as ordinary WKB (which carries its own byte
   order marker).

   The flags byte:
     bit 0      header byte order (1 = little-endian, 0 = big-endian).
                Applies to srs_id and the envelope, not to the WKB.
     bits 1-3   envelope contents indicator (0 = none, 1 = xy, 2 = xyz,
                3 = xym, 4 = xyzm).
     bit 4      empty-geometry flag."
  (:import [org.locationtech.jts.geom Geometry GeometryFactory]
           [org.locationtech.jts.io WKBReader WKBWriter]
           [java.nio ByteBuffer ByteOrder]
           [java.util Arrays]))

(def ^:private envelope-doubles
  "Number of doubles in the header envelope, by envelope indicator code."
  {0 0, 1 4, 2 6, 3 6, 4 8})

(defn reader
  "A WKBReader bound to `factory`, so decoded geometries use its
   PrecisionModel and CoordinateSequenceFactory (see geometry.core's
   patched-csf workaround)."
  ^WKBReader [^GeometryFactory factory]
  (WKBReader. factory))

(defn decode
  "Decode a GeoPackage geometry `blob` into a JTS Geometry using `rdr`
   (a WKBReader, see `reader`). Returns nil for a nil blob. The geometry's
   SRID is set from the blob header."
  ^Geometry [^bytes blob ^WKBReader rdr]
  (when blob
    (let [bb (ByteBuffer/wrap blob)]
      (when-not (and (= 0x47 (bit-and (.get bb 0) 0xff))
                     (= 0x50 (bit-and (.get bb 1) 0xff)))
        (throw (ex-info "Not a GeoPackage geometry blob (bad magic)"
                        {:byte-0 (.get bb 0) :byte-1 (.get bb 1)})))
      (let [flags    (.get bb 3)
            little?  (bit-test flags 0)
            env-code (bit-and (unsigned-bit-shift-right flags 1) 0x07)
            n-env    (or (envelope-doubles env-code)
                         (throw (ex-info "Invalid envelope indicator"
                                         {:envelope-code env-code})))
            header-len (int (+ 8 (* 8 n-env)))
            _        (.order bb (if little? ByteOrder/LITTLE_ENDIAN ByteOrder/BIG_ENDIAN))
            srs-id   (.getInt bb 4)
            ^bytes wkb (Arrays/copyOfRange blob header-len (alength blob))
            ^Geometry geom (.read rdr wkb)]
        (.setSRID geom srs-id)
        geom))))

(defn encode
  "Encode a JTS `geom` as a GeoPackage geometry blob with the given `srid`.
   Writes a little-endian header with no envelope (both spec-valid); the
   layer extent is tracked separately in gpkg_contents. Returns nil for a
   nil geometry."
  ^bytes [^Geometry geom ^long srid]
  (when geom
    (let [wkb    (.write (WKBWriter.) geom)
          flags  (bit-or 0x01 (if (.isEmpty geom) 0x10 0))
          header (-> (ByteBuffer/allocate 8)
                     (.order ByteOrder/LITTLE_ENDIAN))
          _      (doto header
                   (.put (byte 0x47))
                   (.put (byte 0x50))
                   (.put (byte 0))
                   (.put (byte flags))
                   (.putInt (int srid)))
          out    (byte-array (+ 8 (alength wkb)))]
      (System/arraycopy (.array header) 0 out 0 8)
      (System/arraycopy wkb 0 out 8 (alength wkb))
      out)))
