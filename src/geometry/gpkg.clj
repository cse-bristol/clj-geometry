(ns geometry.gpkg
  "Functions for reading and writing geopackages.

  For example

  (with-open [h (gpkg/open my-file)]
     (doseq [f h]
        (println f)))


  The result of gpkg/open is a lazy sequence that is also closeable.
  The entires of the sequence are geometry.feature/Feature instances,
  so they have a geometry, table name & CRS, and then contain whatever
  columns are in the geopackage table they came from.

  You can write a seq of features with gpkg/write, for example

  (with-open [h (gpkg/open my-file :table x)]
      (gpkg/write output-file output-table
         (for [feature h] (assoc feature :foo 1))))

  this will infer the columns & geometry on the output features
  which probably isn't good for production use. In that case you
  will do well to supply the :schema argument to write.
  "
  
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [geometry.feature :as f]
            [geometry.core :as g]
            [taoensso.timbre :as log])
  (:import [org.opengis.feature.simple SimpleFeature]
           [org.opengis.referencing.operation MathTransform]
           [org.geotools.data DataStoreFinder DataUtilities DefaultTransaction]
           [org.geotools.geometry.jts JTS Geometries ReferencedEnvelope]
           [org.locationtech.jts.geom Geometry]
           [org.geotools.referencing CRS]
           [org.geotools.geopkg GeoPackage FeatureEntry]
           [org.sqlite
            SQLiteConfig
            SQLiteConfig$JournalMode
            SQLiteConfig$Pragma
            SQLiteConfig$TransactionMode
            SQLiteConfig$LockingMode]))

(defn table-names [gpkg])

(defn- ->feature [key-transform
                  ^MathTransform crs-transform
                  ^SimpleFeature feature
                  table-name crs]
  (let [geometry-property-name (.getName
                                (.getDefaultGeometryProperty feature))]
    (f/map->Feature
     (persistent!
      (reduce
       (fn [out property]
         (let [is-geometry (= (.getName property) geometry-property-name)]
           (assoc! out
                   (if is-geometry :geometry
                       (key-transform (.getLocalPart (.getName property))))
                   (cond-> (.getValue property)
                     (and is-geometry crs-transform)
                     (JTS/transform crs-transform)))))
       
       (transient {:table table-name :crs crs})
       (.getProperties feature))))))

(defn- ->crs [x]
  (cond (string? x) (CRS/decode x true)
        (integer? x) (CRS/decode (str "EPSG:" x) true)
        :else (throw (IllegalArgumentException. (str "Unknown type of CRS " x)))))

(defn open [gpkg & {:keys [table-name to-crs key-transform]
                    :or {key-transform identity}}]
  (assert (.exists (io/as-file gpkg)))
  (let [store (DataStoreFinder/getDataStore
               {"dbtype" "geopkg"
                "database" (.getCanonicalPath (io/as-file gpkg))})

        _ (try (.setGeometryFactory store g/*factory*) (catch Exception e (log/warn e)))
        _ (try (.setCharset store (java.nio.charset.StandardCharsets/UTF_8))   (catch Exception e))
        
        tables (if table-name
                 [table-name]
                 (into [] (.getTypeNames store)))

        state (volatile! {:table nil
                          :tables tables
                          :crs nil
                          :transform nil
                          :iterator nil
                          :closed false})

        maybe-advance!
        (fn [{:keys [iterator tables] :as state}]
          (when (:closed state) (throw (ex-info "Iterating on a geopackage that has been closed" {:input gpkg :state state})))
          (if (and iterator (.hasNext iterator))
            state ;; just continue with this iterator
            
            (if (seq tables) ;; there are more tables, start next table
              (let [[table & tables] tables
                    source (.getFeatureSource store table)
                    new-iterator (-> source
                                     (.getFeatures)
                                     (.features))
                    crs (-> source .getInfo .getCRS (CRS/lookupIdentifier true))]
                (when iterator (.close iterator))
                (assoc state
                       :table table
                       :tables tables
                       :transform (when to-crs
                                    (let [from-crs (->crs crs)
                                          to-crs (->crs to-crs)]
                                      (CRS/findMathTransform from-crs to-crs true)))
                       :crs crs
                       :iterator new-iterator))

              ;; reached the end
              (do
                (when iterator (.close iterator))
                {:table nil :tables nil :crs nil :iterator nil :closed false}))))

        ;; we don't thread state through here because we want to handle close
        feature-seq
        (fn feature-seq []
          (lazy-seq
           (let [state (vswap! state maybe-advance!)]
             (if (:iterator state)
               (cons (->feature key-transform
                                (:transform state)
                                (.next (:iterator state))
                                (:table state)
                                (:crs state))
                     (feature-seq))))))

        feature-seq (keep identity (feature-seq))
        ]
    
    (reify
      java.lang.AutoCloseable
      (close [_]
        ;; close feature iterator
        (vswap! state (fn [state]
                        (when-let [i (:iterator state)] (.close i))
                        (assoc state :iterator nil :closed true)))
        (.dispose store))

      ;; these are a bit immoral in that the returned
      ;; object closes its closability. but this will be OK
      ;; for the main pattern of (with-open [x ...] (dothings x))
      clojure.lang.ISeq
      (first [_] (.first feature-seq))
      (next [_] (.next feature-seq))
      (more [_] (.more feature-seq))
      (cons [_ x] (.cons feature-seq x))
      (count [_] (.count feature-seq))
      (empty [_] (.empty feature-seq))
      (equiv [_ y] (.equiv feature-seq y))
      (seq [_] (.seq feature-seq)))))

(defn- kv-type [k v]
  [(name k)
   (cond->
       {:accessor #(get % k)
        :type
        (if (instance? Geometry v)
          Geometries/GEOMETRY
          (case (.getCanonicalName (class v))
            ("java.lang.String"
             "java.lang.Float"
             "java.lang.float"
             "java.lang.Double"
             "java.lang.double"
             "java.lang.Boolean"
             "java.lang.boolean"
             "java.lang.Short"
             "java.lang.short"
             "java.lang.Integer"
             "java.lang.int"
             "java.lang.Long"
             "java.util.Date"
             "java.sql.Date"
             "java.sql.Time"
             "java.sql.Timestamp"
             "java.math.BigDecimal") (.getCanonicalName (class v))
            (str name ":String")))}
     (instance? Geometry v)
     (assoc :srid (g/srid v)))])

(defn- infer-spec
  "Given a feature, get a spec for its fields inferring types from their values.
  
  This function should probably only be used interactively or for hacking around,
  as it is not very safe if you might have nil values (which have no type)"
  
  [feature]
  (cond (instance? geometry.feature.Feature feature)
        (let [keys (keys feature)]
          (concat
           [["geometry"
             {:type Geometries/GEOMETRY
              :srid  (g/srid feature)
              :accessor g/geometry}]]
           (for [k (sort-by str keys)
                 :when (and (not= k :geometry)
                            (not= k :table)
                            (not= k :crs))]
             (kv-type k (get feature k)))))
        
        (satisfies? g/HasGeometry feature)
        [["geometry"
          {:type Geometries/GEOMETRY
           :accessor g/geometry
           :srid (g/srid feature)}]]
        
        (map? feature)
        (for [[k v] (sort-by (comp str first) feature)] (kv-type k v))

        :else
        (throw (ex-info "Unable to infer schema for value" {:value feature}))))

(def ^:private sqlite-config
  (doto (SQLiteConfig.)
    (.setJournalMode SQLiteConfig$JournalMode/WAL)
    (.setPragma SQLiteConfig$Pragma/SYNCHRONOUS "OFF")
    (.setTransactionMode SQLiteConfig$TransactionMode/DEFERRED)
    (.setReadUncommited true)
    ;; (.setLockingMode SQLiteConfig$LockingMode/EXCLUSIVE)
    (.setPragma SQLiteConfig$Pragma/MMAP_SIZE
                (str (* 1024 1024 1024)) ;; 1G
                )))

(defn- open-for-writing [file batch-insert-size]
  (let [geopackage (GeoPackage. (io/as-file file) sqlite-config nil)]
    (.init geopackage)
    (let [m (.getDeclaredMethod GeoPackage "dataStore" nil)]
      (.setAccessible m true)
      (let [ds (.invoke m geopackage nil)]
        (.setBatchInsertSize ds batch-insert-size)))
    geopackage))

(defn- spec-geom-field [spec]
  (first (filter
          (fn [[k {:keys [type]}]]
            (or
             (= Geometries/GEOMETRY type)
             (= Geometries/GEOMETRYCOLLECTION type)
             (= Geometries/LINESTRING type)
             (= Geometries/MULTILINESTRING type)
             (= Geometries/MULTIPOINT type)
             (= Geometries/MULTIPOLYGON type)
             (= Geometries/POINT type)
             (= Geometries/POLYGON type)

             (= :geometry type)
             (= :geometry-collection type)
             (= :line-string type)
             (= :multi-line-string type)
             (= :multi-point type)
             (= :multi-polygon type)
             (= :point type)
             (= :polygon type)))
          spec)))

(defn- ->feature-entry [table-name spec]
  (let [geom-col (spec-geom-field spec)]
    (doto (FeatureEntry.)
      (.setTableName table-name)
      (.setGeometryColumn (first geom-col))
      (.setGeometryType (->geotools-type (:type (second geom-col)))))))


(defn- ->geotools-type [type]
  (cond
    (class? type)  (.getCanonicalName type)
    (string? type) type

    (= :geometry type) Geometries/GEOMETRY
    (= :geometry-collection type) Geometries/GEOMETRYCOLLECTION
    (= :line-string type) Geometries/LINESTRING
    (= :multi-line-string type) Geometries/MULTILINESTRING
    (= :multi-point type) Geometries/MULTIPOINT
    (= :multi-polygon type) Geometries/MULTIPOLYGON
    (= :point type) Geometries/POINT
    (= :polygon type) Geometries/POLYGON

    :else type))

(defn- ->geotools-schema [table-name spec]
  (DataUtilities/createType
   table-name
   (string/join ","
                (for [[name {:keys [type srid]}] spec]
                  (let [type (->geotools-type type)]
                    (str name ":" type (when srid (str ":srid=" srid))))))))

(defn write
  "Write the given sequence of `features` into a geopackage at `file`
  in a table called `table-name`

  The `schema` argument is good to supply; without it the schema will be
  inferred which may not be what you want.

  A schema looks like a series of tuples, like

  [field-name field-attributes]

  field-attributes is a map having :type, :accessor and optionally :srid
  for geometry types.

  :type is the canonical name of a java class, or just :String, or Geometries/GEOMETRY.

  Look at `infer-spec` for examples.
  "
  ([file table-name features & {:keys [schema batch-insert-size]
                                :or {batch-insert-size 4000}}]
   (with-open [geopackage (open-for-writing file batch-insert-size)]
     (let [features      (reductions (fn [_ x] x) features)
           spec          (vec (or schema (infer-spec (first features))))
           getters       (mapv (fn [[k v]]
                                 (:accessor v #(get % k)))
                               spec)
           feature-entry (->feature-entry table-name spec)
           _ (def -last-spec spec)
           [_ {:keys [srid]}]          (spec-geom-field spec)]
         (.setBounds feature-entry
                     (ReferencedEnvelope. 0 0 0 0 (CRS/decode (str "EPSG:" srid))))
         (try
           (.create geopackage feature-entry (->geotools-schema table-name spec))
           (catch java.lang.IllegalArgumentException _))

         ;; TODO should we have a transaction around the whole lot?
         (with-open [tx (DefaultTransaction.)]
           (try 
             (with-open [writer (.writer geopackage feature-entry true nil tx)]
               (run!
                (fn [feature]
                  (.setAttributes
                   (.next writer)
                   (mapv (fn [getter] (getter feature)) getters))
                  (.write writer))
                features))
             
             (.commit tx)
             (catch Exception e (.rollback tx) (throw e))))))))


