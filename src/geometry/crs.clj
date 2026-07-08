(ns geometry.crs
  "CRS lookup and reprojection backed by proj4j (with the proj4j-epsg
   dataset)."
  (:import [org.locationtech.proj4j
            CRSFactory CoordinateReferenceSystem
            CoordinateTransformFactory CoordinateTransform ProjCoordinate]
           [org.locationtech.jts.geom Geometry Coordinate CoordinateFilter]
           [org.locationtech.proj4j.proj LongLatProjection]))

(def ^:private ^CRSFactory crs-factory (CRSFactory.))
(def ^:private ^CoordinateTransformFactory transform-factory (CoordinateTransformFactory.))

(def lookup
  "Look up a CoordinateReferenceSystem by EPSG `srid`, or nil if proj4j
   does not know it. Memoized."
  (memoize
   (fn ^CoordinateReferenceSystem [srid]
     (try (.createFromName crs-factory (str "EPSG:" srid))
          (catch Exception _ nil)))))

(defn srs-row
  "Row data for gpkg_spatial_ref_sys describing EPSG `srid`. The CRS is
   identified by organization/organization_coordsys_id (which is what
   GDAL/QGIS actually use); `definition` carries the proj4 parameter
   string from the bundled EPSG dataset, or \"undefined\" if unknown."
  [srid]
  (if-let [^CoordinateReferenceSystem crs (lookup srid)]
    {:srs_name (.getName crs)
     :organization "EPSG"
     :organization_coordsys_id srid
     :definition (.getParameterString crs)
     :description (.getName crs)}
    {:srs_name (str "EPSG:" srid)
     :organization "EPSG"
     :organization_coordsys_id srid
     :definition "undefined"
     :description nil}))

(defn transform
  "A CoordinateTransform from EPSG `from-srid` to EPSG `to-srid`."
  ^CoordinateTransform [from-srid to-srid]
  (let [from (lookup from-srid)
        to   (lookup to-srid)]
    (when-not from (throw (ex-info "Unknown source CRS" {:srid from-srid})))
    (when-not to (throw (ex-info "Unknown target CRS" {:srid to-srid})))
    (.createTransform transform-factory from to)))

(defn- geographic?
  "True if `crs` is an unprojected geographic CRS whose EPSG axis order
   is (latitude, longitude) — i.e. x and y must be swapped relative to
   proj4j's internal (longitude, latitude) convention."
  [^CoordinateReferenceSystem crs]
  (instance? LongLatProjection (.getProjection crs)))

(defn reproject
  "Return a copy of JTS `geom` with every coordinate transformed by
   `^CoordinateTransform t`. Mutates a clone, leaving `geom` untouched.
   Sets the result SRID to `to-srid`.

   For 4326, x=longitude, y=latitude."
  ^Geometry [^Geometry geom ^CoordinateTransform t to-srid]
  (let [out      (.copy geom)
        src      (ProjCoordinate.)
        dst      (ProjCoordinate.)]
    (.apply out
            (reify CoordinateFilter
              (filter [_ c]
                (let [^Coordinate c c]
                  (set! (.-x src) (.-x c))
                  (set! (.-y src) (.-y c))
                  (.transform t src dst)
                  (set! (.-x c) (.-x dst))
                  (set! (.-y c) (.-y dst))
                  nil))))
    (.geometryChanged out)
    (.setSRID out (int to-srid))
    out))
