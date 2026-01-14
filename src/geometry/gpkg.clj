(ns geometry.gpkg
  "Functions for reading and writing geopackages or non-spatial
   sqlite tables.

  For example

  (with-open [h (gpkg/open my-file)]
     (doseq [f (gpkg/features h)]
        (println f)))


  The result of gpkg/open is a lazy sequence that is also closeable.
  The entires of the sequence are geometry.feature/Feature instances,
  so they have a geometry, table name & CRS, and then contain whatever
  columns are in the geopackage table they came from.

  You can write a seq of features with gpkg/write, for example

  (with-open [h (gpkg/open my-file :table x)]
      (gpkg/write output-file output-table
         (for [feature (gpkg/features h)] (assoc feature :foo 1))))

  this will infer the columns & geometry on the output features,
  which should be avoided in production, as it won't work properly
  if any columns have null values in the first row. In that case you
  will do well to supply the :schema argument to write.
  "

  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.set :as set]
            [geometry.core :as g]
            [geometry.feature :as f]
            [next.jdbc :as jdbc]
            [taoensso.timbre :as log])
  (:import [org.geotools.data DataStoreFinder DataUtilities DefaultTransaction]
           [org.geotools.feature.simple SimpleFeatureImpl]
           [org.geotools.feature.simple SimpleFeatureImpl$Attribute]
           [org.geotools.geometry.jts Geometries JTS ReferencedEnvelope]
           [org.geotools.geopkg FeatureEntry GeoPackage]
           [org.geotools.jdbc JDBCDataStore JDBCFeatureStore]
           [org.geotools.referencing CRS]
           [org.locationtech.jts.geom Geometry Envelope]
           [org.opengis.referencing.operation MathTransform]
           [org.geotools.jdbc JDBCFeatureReader$ResultSetFeature]
           [org.sqlite
            SQLiteConfig
            SQLiteConfig$JournalMode
            SQLiteConfig$Pragma
            SQLiteConfig$TransactionMode]
           [java.util.logging Level]))

;; SHUT UP PLEASE. I note that this is considered bad, because it
;; means we cannot programmatically override this at runtime but
;; frankly I am not sure we want that ability.
;; All the alternative solutions for this are inordinately complicated.
(doto (org.geotools.util.logging.Logging/getLogger
       org.geotools.jdbc.JDBCDataStore)
  (.setLevel Level/SEVERE))

(defn- sqlite-query! [file query-params]
  (with-open [conn (jdbc/get-connection
                    {:dbtype "sqlite"
                     :dbname (.getCanonicalPath (io/as-file file))})]
    (jdbc/with-transaction [tx conn]
      (jdbc/execute! tx query-params))))

(defn table-names
  "Get the tables present in the geopackage or sqlite database, as a
   set of strings."
  [file & {:keys [spatial-only? include-system?]
           :or {spatial-only? false include-system? false}}]
  (let [file (io/as-file file)]
    (if spatial-only?
      (let [geopackage (GeoPackage. file)
            store (DataStoreFinder/getDataStore
                   {"dbtype" "geopkg"
                    "database" (.getCanonicalPath (io/as-file file))})]
        (try
          (set (.getTypeNames store))
          (finally
            (.dispose store)
            (.close geopackage))))
      (->> [(if include-system?
               "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
               "SELECT name FROM sqlite_master WHERE type IN ('table','view')
                     AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'gpkg_%'
                     AND name NOT LIKE 'rtree_%'")]
           (sqlite-query! file)
           (map :sqlite_master/name)
           (set)))))

(defn- ->crs [x]
  (cond (string? x) (CRS/decode x true)
        (integer? x) (CRS/decode (str "EPSG:" x) true)
        :else (throw (IllegalArgumentException. (str "Unknown type of CRS " x)))))


(defn- gpkg-iterator
  "Returns a Closable Iterator over a geotools FeatureIterator
   for the features in the table.

   Internal implementation detail of gpkg/open."
  [^JDBCFeatureStore source table-name crs ^MathTransform crs-transform key-transform fetch-rowid]
  (let [features (-> source
                     (.getFeatures)
                     (.features))]

    ;; This is a hack to work around a bug/limitation in geotools
    ;; Geotools assumes that all geopackage fields with mime-type application/json
    ;; contain json-encoded primitive arrays, rather than arbitrary blobs
    ;; and therefore decodes them to arrays. I'm not sure whether a geopackage
    ;; can be marked up as containing an int array, and what would happen
    ;; in that case.
    (doseq [att (.getAttributeDescriptors (.getSchema source))]
      (when-let [data-column (some-> att
                                     (.getUserData)
                                     (.get "gpkg.dataColumn"))]
        (when (= "application/json" (.getMimeType data-column))
          ;; this is essentially to disable array decoding in GeoPkgDialect.convertValue() here
          ;; https://github.com/geotools/geotools/blob/e24ceff09ce11f537a666bed2f6c039144177bb3/modules/plugin/geopkg/src/main/java/org/geotools/geopkg/GeoPkgDialect.java#L968
          ;; via https://github.com/geotools/geotools/blob/e24ceff09ce11f537a666bed2f6c039144177bb3/modules/plugin/geopkg/src/main/java/org/geotools/geopkg/GeoPkgDialect.java#L746

          ;; otherwise non-array json values will crash geotools on reading a feature.
          ;; They come out as a String[] with 1 element. This cannot be patched around
          ;; here because the binding is not mutable
          (.setMimeType data-column ""))))
    
    (reify
      java.util.Iterator
      (next [_]
        (when-let [feature ^SimpleFeatureImpl (when (.hasNext features) (.next features))]
          (let [geometry-property-name (.getName
                                        (.getDefaultGeometryProperty feature))]
            (f/map->Feature
             (persistent!
              (reduce
               (fn [out ^SimpleFeatureImpl$Attribute property]
                 (let [is-geometry (= (.getName property) geometry-property-name)]
                   (if-let [col-key (if is-geometry
                                      :geometry
                                      (key-transform (.getLocalPart (.getName property))))]
                     (assoc! out
                             col-key
                             (if (and is-geometry crs-transform)
                               (JTS/transform ^Geometry (.getValue property) crs-transform)
                               (let [value (.getValue property)]
                                 ;; if the value is a string[] of length 1,
                                 ;; unwrap it as it is actually a json value.
                                 (if (and (instance? String/1 value)
                                          (= (count value) 1))
                                   (aget value 0)
                                   value))))
                     ;; do nothing, skip key
                     out)))

               (transient (cond-> {:table table-name :crs crs}
                            fetch-rowid
                            (assoc ::rowid (some-> feature (.getID) (string/split #"\.") (last) (parse-long)))))
               (.getProperties feature)))))))

      (hasNext [_] (.hasNext features))

      java.io.Closeable
      (close [_] (.close features)))))

(defn- sqlite-iterator
  "Returns a Closable Iterator over a ResultSet containing all
   the rows and columns in the table.

   Internal implementation detail of gpkg/open."
  [^java.io.File file ^String table key-transform fetch-rowid]
  (let [ds   (jdbc/get-datasource
              (format "jdbc:sqlite:file:%s?mode=ro&immutable=1"
                      (.getCanonicalPath (io/as-file file))))

        ;; unfortunately next.jdbc is not suitable
        ;; for streaming results as a lazy seq
        conn ^java.sql.Connection (jdbc/get-connection ds)
        stmt ^java.sql.Statement  (.createStatement conn)
        rs   ^java.sql.ResultSet  (.executeQuery
                                   stmt
                                   (if fetch-rowid
                                     (format "SELECT rowid, * FROM \"%s\"" table)
                                     (format "SELECT * FROM \"%s\"" table)))
        md   ^java.sql.ResultSetMetaData (.getMetaData rs)
        cols (vec (for [i (range (if fetch-rowid 2 1) (inc (.getColumnCount md)))
                        :let [col-key (key-transform (.getColumnName md i))]
                        :when col-key]
                    [i col-key]))
        asked-has-next (atom false)
        has-next (atom false)]
    (reify
      java.util.Iterator
      (next [_]
        (when-not @asked-has-next
          (.next rs))
        (reset! asked-has-next false)
        (f/map->Feature
         (persistent!
          (reduce
           (fn [a [^int i n]] (assoc! a n (.getObject rs i)))
           (transient (cond-> {:table table}
                        fetch-rowid (assoc ::rowid (.getObject rs 1))))
           cols))))

      (hasNext [_]
        (when-not @asked-has-next
          (reset! has-next (.next rs))
          (reset! asked-has-next true))
        @has-next)

      java.io.Closeable
      (close [_]
        (.close rs)
        (.close stmt)
        (.close conn)))))

(defn features
  "Given the result of calling `gpkg/open`, convert the closable iterator
   that returns into a lazy sequence of features. This is just a wrapper around
   iterator-seq."
  [gpkg]
  (iterator-seq gpkg))

(defn open
  "The result of gpkg/open is a closeable iterator. It can be turned into
   a lazy sequence using gpkg/features (which is just a wrapper around
   iterator-seq).
   The entries of the iterator are geometry.feature/Feature instances,
   so they have a :geometry, :table :crs, and then contain whatever
   columns are in the geopackage table they came from.

   for example you can print a geopackage out with

   (with-open [h (gpkg/open my-file)]
     (doseq [f (gpkg/features h)]
        (println f)))

   If you want a specific table, use the `table` keyword argument.

   Column names are processed through `key-transform`, so e.g. `keyword`
   will convert them to clojure keywords. If `key-transform` returns false/nil
   the column will be omitted from the feature, so you can use it to select
   specific columns as well. For example 

   :key-transform (comp #{:a :b} keyword) will keep columns a and b

   if `rowids?` is true, the resulting entries will also
   have :geometry.gpkg/rowid on them, containing the sqlite internal
   primary key of the row. For spatial tables this is defined to be the
   feature ID (fid).

   If you know the geometries have a certain precision, you can
   pass a GeometryFactory with a PrecisionModel set. If you do
   not know the precision of the geometries, but want them to be
   of a certain precision, use core/change-precision instead."
  [gpkg & {:keys [table-name to-crs key-transform geometry-factory spatial-only?
                  rowids?]
           :or {key-transform identity
                geometry-factory g/*factory*}}]
  (assert (.exists (io/as-file gpkg)))
  (let [store ^JDBCDataStore (DataStoreFinder/getDataStore
                              {"dbtype" "geopkg"
                               "database" (.getCanonicalPath (io/as-file gpkg))
                               "read-only" true})

        _ (try (.setGeometryFactory store geometry-factory) (catch Exception e (log/warn e)))

        tables (if table-name
                 (let [known (table-names gpkg :include-system? true)]
                   (when-not (known table-name)
                     (throw (ex-info "Table not found in geopackage"
                                     {:file gpkg
                                      :table table-name
                                      :tables known})))
                   [table-name])
                 (into [] (table-names gpkg :spatial-only? spatial-only?)))

        state (volatile! {:table nil
                          :tables tables
                          :iterator nil
                          :closed false})

        maybe-advance!
        (fn maybe-advance! [{:keys [^java.util.Iterator iterator tables] :as state}]
          (when (:closed state) (throw (ex-info "Iterating on a geopackage that has been closed" {:input gpkg :state state})))
          (if (and iterator (.hasNext iterator))
            state ;; just continue with this iterator

            (if (seq tables) ;; there are more tables, start next table
              (let [[^String table & tables] tables
                    source ^JDBCFeatureStore (try
                                               (.getFeatureSource store table)
                                               (catch Exception e
                                                 (if (re-find #"Schema '.+' does not exist." (.getMessage e))
                                                   nil
                                                   (throw e))))]
                (when iterator (.close iterator))
                (let [next-state
                      (if source
                        ;; spatial table
                        (let [crs (-> source .getInfo .getCRS (CRS/lookupIdentifier true))
                              crs-transform (when to-crs
                                              (let [from-crs (->crs crs)
                                                    to-crs (->crs to-crs)]
                                                (CRS/findMathTransform from-crs to-crs true)))]
                          (assoc state
                                 :table table
                                 :tables tables
                                 :iterator (gpkg-iterator source table crs crs-transform key-transform rowids?)))
                        ;; non-spatial table
                        (assoc state
                               :table table
                               :tables tables
                               :iterator (sqlite-iterator gpkg table key-transform rowids?)))]

                  ;; this is to deal with empty tables - the Iterator implementation that's returned
                  ;; below mustn't (not (.hasNext)) until we have got to the end, so if we've made an
                  ;; iterator for a table with nothing in we need to move onto the next table
                  (if (and (:iterator next-state)
                           (not (.hasNext (:iterator next-state))))
                    (maybe-advance! next-state)
                    next-state)))

              ;; reached the end
              (do
                (when iterator (.close iterator))
                {:table nil :tables nil :iterator nil :closed false}))))
        ]

    (reify
      java.lang.AutoCloseable
      (close [_]
        ;; close feature iterator
        (try (vswap! state (fn [state]
                             (when-let [^java.io.Closeable i (:iterator state)] (.close i))
                             (assoc state :iterator nil :closed true)))
             (finally (.dispose store))))

      java.util.Iterator
      (next [_]
        (let [state (vswap! state maybe-advance!)]
          (when-let [^java.util.Iterator iterator (:iterator state)]
            (.next iterator))))

      (hasNext [_]
        (let [state (vswap! state maybe-advance!)]
          (if-let [^java.util.Iterator iterator (:iterator state)]
            (.hasNext iterator)
            false)))
      )))

(defn- kv-type [k v]
  [(name k)
   (cond->
       {:accessor (if (keyword? k) k #(get % k))
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

(def ^:private ^SQLiteConfig sqlite-config
  "SQLite config, optimised for write speed."
  (doto (SQLiteConfig.)
    (.setJournalMode SQLiteConfig$JournalMode/WAL)
    (.setPragma SQLiteConfig$Pragma/SYNCHRONOUS "OFF")
    (.setTransactionMode SQLiteConfig$TransactionMode/DEFERRED)
    (.setReadUncommited true)
    ;; (.setLockingMode SQLiteConfig$LockingMode/EXCLUSIVE)
    (.setPragma SQLiteConfig$Pragma/MMAP_SIZE
                (str (* 1024 1024 1024)) ;; 1G
                )))

(defn- open-for-writing
  "Open a gpkg for writing spatial data via the geotools APIs.

   Also sets the batch insert size (via reflection)."
  ^GeoPackage [file batch-insert-size]
  (let [geopackage (GeoPackage. (io/as-file file) sqlite-config nil)]
    (.init geopackage)
    (let [m (.getDeclaredMethod GeoPackage "dataStore" nil)]
      (.setAccessible m true)
      (let [ds (.invoke m geopackage nil)]
        (.setBatchInsertSize ds batch-insert-size)))
    geopackage))

(defn- open-sqlite
  "Open a gpkg for general SQL operations via JDBC"
  [file]
  (org.sqlite.JDBC/createConnection
   (format "jdbc:sqlite:%s"
           (.getCanonicalPath (io/as-file file)))
   (.toProperties sqlite-config)))

(defn- spec-geom-field
  "Get the spec for the (first) geometry field from a schema."
  [spec]
  (first (filter
          (fn [[_ {:keys [type]}]]
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

(defn- ->geotools-type
  "Convert a type specification from a schema into something that geotools can understand
   as a column type."
  [type]
  (cond
    (class? type)  (.getCanonicalName ^Class type)
    (string? type) type

    (= :geometry type) Geometries/GEOMETRY
    (= :geometry-collection type) Geometries/GEOMETRYCOLLECTION
    (= :line-string type) Geometries/LINESTRING
    (= :multi-line-string type) Geometries/MULTILINESTRING
    (= :multi-point type) Geometries/MULTIPOINT
    (= :multi-polygon type) Geometries/MULTIPOLYGON
    (= :point type) Geometries/POINT
    (= :polygon type) Geometries/POLYGON

    (= :integer type) (.getCanonicalName Integer)
    (= :int type) (.getCanonicalName Integer)
    (= :long type) (.getCanonicalName Long)
    (= :short type) (.getCanonicalName Short)
    (= :float type) (.getCanonicalName Float)
    (= :double type) (.getCanonicalName Double)
    (= :real type) (.getCanonicalName Double)
    (= :string type) (.getCanonicalName String)
    (= :boolean type) (.getCanonicalName Boolean)

    (keyword? type) (.getCanonicalName String)

    :else type))

(defn- ->feature-entry
  "Create a geotools FeatureEntry (for our purposes a gpkg table definition)"
  [table-name spec]
  (let [geom-col (spec-geom-field spec)]
    (doto (FeatureEntry.)
      (.setTableName table-name)
      (.setGeometryColumn (name (first geom-col)))
      (.setGeometryType (->geotools-type (:type (second geom-col)))))))

(defn- ->geotools-schema
  "Create a geotools schema that defines the columns of a gpkg table."
  [table-name spec]
  (DataUtilities/createType
   table-name
   (string/join ","
                (for [[name {:keys [type srid]}] spec]
                  (let [type (->geotools-type type)]
                    (str (clojure.core/name name) ":" type (when srid (str ":srid=" srid))))))))

(defn- set-layer-extent!
  "Update the extent (if not nil) for `table-name` in the geopackage at `file`

  return `file` in case you want to thread"
  [file table-name  ^ReferencedEnvelope layer-extent]
  (when layer-extent
    (sqlite-query!
     file
     ["UPDATE gpkg_contents SET min_x = ?, min_y = ?, max_x = ?, max_y = ? WHERE table_name = ?;"
      (.getMinX layer-extent)
      (.getMinY layer-extent)
      (.getMaxX layer-extent)
      (.getMaxY layer-extent)
      table-name]))
  file)

(defn- escape-identifier
  "Return a version of `identfier` safe for use in an sql string.

  This will quote it and escape any embedded quotes. Needed because
  table/column names cannot be query parameters. next.jdbc has
  functions like this but they do not support field names that contain
  quotes, or periods."
  ([identifier]
   (str \" (string/replace (name identifier) #"\"" "\"\"") \"))
  ([table col]
   (str (escape-identifier table) "." (escape-identifier col))))

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

   Returns nil.
  "
  ([file table-name ^Iterable features & {:keys [schema batch-insert-size
                                                 add-spatial-index
                                                 if-exists]
                                          :or {batch-insert-size 4000
                                               if-exists :append}}]
   {:pre [(or (instance? Iterable features) (nil? features))
          (#{:drop-table :delete-rows :append} if-exists)]}

   (cond
     (= :drop-table if-exists)
     (try (with-open [ds (DataStoreFinder/getDataStore
                          {"dbtype" "geopkg"
                           "database" (.getCanonicalPath (io/as-file file))})]
            (.removeSchema ds table-name)
            (.dispose ds))
          (catch Exception _
            ;; this is only safe on a non-spatial table
            (with-open [conn (open-sqlite file)]
              (jdbc/with-transaction [tx conn]
                (jdbc/execute!
                 tx
                 [(format "DROP TABLE IF EXISTS %s" (escape-identifier table-name))])))))
     
     (= :delete-rows if-exists)
     (with-open [conn (open-sqlite file)]
       (try
         (jdbc/with-transaction [tx conn]
           (jdbc/execute!
            tx
            [(format "DELETE FROM %s" (escape-identifier table-name))]))
         (catch Exception _))))
   
   (with-open [geopackage (open-for-writing file batch-insert-size)]
     (let [spec (vec (or schema (infer-spec (first features))))
           [geom-field {:keys [srid]
                        geom-accessor :accessor
                        :or   {srid 27700}}] (spec-geom-field spec)
           crs (CRS/decode (str "EPSG:" srid))]
       (if geom-field
         ;; spatial data:
         (let [emit-feature (let [getters
                                  (vec (for [[k v] spec]
                                         (or (:accessor v)
                                             #(get % k))))]
                              (fn [feature]
                                (mapv #(% feature) getters)))
               feature-entry ^FeatureEntry (->feature-entry table-name spec)]
           (.setBounds feature-entry (ReferencedEnvelope. 0 0 0 0 crs))
           (try
             (.create geopackage feature-entry (->geotools-schema table-name spec))
             (catch java.lang.IllegalArgumentException _))

           (when add-spatial-index
             (try (.createSpatialIndex geopackage feature-entry)
                  (catch java.io.IOException e
                    (log/warnf e "Unable to create spatial index in %s on %s" file table-name))))
           
           
           ;; It may seem odd that we are getting an iterator out here
           ;; rather than just doing doseq [feature features], but for
           ;; reasons Neil & Tom were unable to discern that prevents
           ;; garbage collection of features (even though it looks
           ;; eligible for locals clearing).
           (let [features (or features [])
                 iter ^java.util.Iterator (.iterator ^java.lang.Iterable features)

                 ;; It is important that there are two with-opens here.
                 ;; because the ordering of events has to be
                 ;; (.close writer)
                 ;; (.commit tx)
                 ;; (.close tx)

                 ^ReferencedEnvelope layer-extent
                 (with-open [tx (DefaultTransaction.)]
                   (let [extent
                         (with-open [writer (.writer geopackage feature-entry true nil tx)]
                           (loop [^ReferencedEnvelope extent nil]
                             (if (.hasNext iter)
                               (let [feature (.next iter)
                                     ]
                                 (.setAttributes
                                  ^JDBCFeatureReader$ResultSetFeature (.next writer)
                                  ^java.util.List (emit-feature feature))
                                 (.write writer)
                                 (recur
                                  (let [^Geometry geom (if geom-accessor
                                                         (geom-accessor feature)
                                                         (get feature geom-field))
                                        ^Envelope feature-env
                                        (cond
                                          (nil? geom) nil
                                          (g/point? geom) (Envelope. (.getCoordinate geom))
                                          :else (Envelope. (.getEnvelopeInternal geom)))]
                                    (cond
                                      (nil? feature-env) extent

                                      (nil? extent) (ReferencedEnvelope. feature-env crs)

                                      :else (doto extent (.expandToInclude feature-env))))
                                  
                                  ))
                               extent ;; return extent
                               )))]
                     (.commit tx)
                     extent ;; and return extent
                     ))]

             ;; Update the layer extent manually
             (set-layer-extent! file table-name layer-extent)))

         ;; non-spatial data:
         (let [col-types (string/join
                          ","
                          (for [[col {col-type :type}] spec]
                            (str (escape-identifier col) " "
                                 ;; handled values should match ->geotools-type (except for geometric types):
                                 (cond
                                   (#{:int :integer :short :long Integer Short Long
                                      (.getCanonicalName Integer)
                                      (.getCanonicalName Short)
                                      (.getCanonicalName Long)} col-type) "INTEGER"
                                   (#{:double :float :real Double Float
                                      (.getCanonicalName Double)
                                      (.getCanonicalName Float)} col-type) "REAL"
                                   (#{:boolean Boolean
                                      (.getCanonicalName Boolean)} col-type) "BOOLEAN"
                                   :else "TEXT"))))

               table-stmt (format "CREATE TABLE IF NOT EXISTS %s (%s);"
                                  (escape-identifier table-name) col-types)

               cols (mapv (comp name first) spec)

               insert-stmt (format "INSERT INTO %s (%s) values (%s)"
                                   (escape-identifier table-name)
                                   (string/join ", " (map escape-identifier cols))
                                   (string/join ", " (repeat (count cols) "?")))]

           ;; have to make the connection manually so that SQLite config can be passed:
           (with-open [conn (open-sqlite file)]
             (jdbc/with-transaction [tx conn]
               (jdbc/execute! tx [table-stmt])
               (jdbc/execute-batch!
                tx
                insert-stmt
                (for [feature features]
                  (for [[col {accessor :accessor}] spec]
                    (if (nil? accessor)
                      (get feature col)
                      (accessor feature))))
                {:batch-size batch-insert-size})))))
       nil))))

(defn amend
  "Update existing rows in `table` within `gpkg`.
  `values` are the new values to write in. They should have ::rowid on
  them so we can join. `gpkg/read` above will extract rowid if you ask
  for it.

  The expected usage is that you pull rows out with `read` including
  rowid, update / create some columns, and then call `amend` with a
  schema for the columns to be updated.

  `if-exists` can be `:preserve`, `:set-null` or `:drop-column`.

  `:drop-column` is safest, since you don't need to be sure the types
  align. However, older sqlite drivers do not support DROP COLUMN in
  ALTER TABLE, and the verison of geotools we're using currently
  depends on an old sqlite. So we either need to risk using variant
  versions of sqlite, or update geotools.
  
  `method` can be `:update-set`, `:left-join`, `:outer-join`

  - `:update-set` works as UPDATE table SET cols = vals WHERE table.rowid = input.rowid
    
    unmatched rows are affected according to `if-exists`
    the inputs must be distinct on ::rowid
  - `:left-join` works as CREATE table AS (SELECT cols FROM old_table LEFT JOIN input)

    unmatched rows are affected according to `if-exists`
    if inputs are not distinct on ::rowid, new rows are created. old rowids are broken
    so any internal joins / triggers etc related to these will be damaged.
  - `:outer-join` does `:left-join` and then inserts unmatched rows

  The actual implementation here is:
  1. insert data into a temp table
  2. (if needed) zap the existing columns
  3. add any missing columns
  4. update the temp table to mark repeat rowids (for left-join type behaviour)
  5. issue UPDATE SET to copy cols for non-repeating rows
  6. issue INSERT INTO (SELECT FROM JOIN) for repeating rows
  7. issue DELETE for original copies of repeating rows

  This routine will at least preserve indexes and FK relationships on the target table.

  It may fail if the result is not valid.
  "
  [gpkg table values
   & {:keys [schema if-exists method]
      :or {if-exists :set-null method :update-set}}]

  {:pre [(#{:update-set :left-join :outer-join} method)]}
  
  (let [escape-identifier (memoize escape-identifier)
        schema (or schema (infer-spec (first values)))
        temp-table "__temp__"
        rowid-col "_original_rowid"]

    ;; first write the new columns into our temporary table
    (write
     gpkg temp-table values
     :schema (conj
              schema
              [rowid-col {:type :long :accessor ::rowid}]))

    (with-open [conn (open-sqlite gpkg)]
      (try
        (jdbc/with-transaction [tx conn]
          (jdbc/execute!
           tx [(format "CREATE INDEX __temp__rowid__idx ON %s (%s)"
                       (escape-identifier temp-table)
                       (escape-identifier rowid-col))])
          
          ;; ensure target columns exist; first we need to find out
          ;; what columns there are, then to drop or null them out
          ;; (maybe), then to create what is missing
          (let [src-cols (->> (jdbc/execute!
                               tx [(format "PRAGMA table_info(%s)" (escape-identifier temp-table))])
                              (remove (comp #{rowid-col "rowid" "fid"} :name)))

                src-names (set (map :name src-cols))
                
                tgt-names (->> (jdbc/execute!
                                tx [(format "PRAGMA table_info(%s)" (escape-identifier table))])
                               (map :name)
                               (set))

                existing (set/intersection src-names tgt-names)
                missing  (if (= if-exists :drop-column) ;; if we are going to drop them, we need to create them all!
                           src-names
                           (set/difference src-names tgt-names))]

            (cond (= :drop-column if-exists)
                  (doseq [col existing]
                    (jdbc/execute!
                     tx
                     [(let [s (format "ALTER TABLE %s DROP COLUMN %s" (escape-identifier table) (escape-identifier col))]
                        (println s)
                        s
                        )]))
                  
                  (and (seq existing) (= :set-null if-exists))
                  (jdbc/execute!
                   tx
                   [(format "UPDATE %s SET %s"
                            (escape-identifier table)
                            (string/join
                             ", "
                             (for [col existing] (str (escape-identifier col) " = NULL"))))]))

            
            
            (doseq [col src-cols]
              (when (missing (:name col))
                (jdbc/execute!
                 tx
                 [(format "ALTER TABLE %s ADD COLUMN %s %s"
                          (escape-identifier table)
                          (escape-identifier (:name col))
                          (:type col))])))

            (jdbc/execute!
             tx
             [(format "ALTER TABLE %s ADD COLUMN __singular BOOLEAN default TRUE" (escape-identifier temp-table))])

            (let [non-singular-count
                  (-> (jdbc/execute!
                       tx
                       [(format "UPDATE %s SET __singular = false FROM (SELECT %s jid, count(*) N FROM %s GROUP BY %s) B WHERE %s = B.jid AND B.N > 1"
                                (escape-identifier temp-table) ;; update %s set
                                (escape-identifier rowid-col) ;; %s jid
                                (escape-identifier temp-table) ;; from %s
                                (escape-identifier temp-table rowid-col) ;; group by %s
                                (escape-identifier temp-table rowid-col))]) ;; where %s = 
                      (first)
                      (:next.jdbc/update-count))]
              (when (and (pos? non-singular-count)
                         (= :update-set method))
                (throw (ex-info
                        "Multiple matched rows in geopackage update"
                        {:duplicate-row-count non-singular-count
                         :table table
                         :file gpkg
                         :values values}))))

            ;; update-set always happens
            (jdbc/execute!
             tx
             [(format "UPDATE %s SET %s FROM %s WHERE %s = %s AND %s.__singular"
                      (escape-identifier table)
                      (string/join
                       ", "
                       (for [col src-cols]
                         (str (escape-identifier (:name col)) " = " (escape-identifier temp-table (:name col)))))
                      (escape-identifier temp-table)
                      (escape-identifier table "rowid") (escape-identifier temp-table rowid-col)
                      (escape-identifier temp-table))])
            
            (when (or (= method :left-join) (= method :outer-join))
              ;; left join and outer join both need the multiplied up rows inserting
              (let [set-cols ;; all the cols we are setting
                    (for [col src-cols]
                      [(escape-identifier (:name col))
                       (format "(CASE WHEN %s NOT NULL THEN %s ELSE %s END) AS %s"
                               (escape-identifier temp-table rowid-col)
                               (escape-identifier temp-table (:name col))
                               (escape-identifier table (:name col))
                               (escape-identifier (:name col)))])

                    keep-cols ;; all the cols we are just keeping
                    (for [col-name (disj (set/difference tgt-names src-names) "fid")]
                      [(escape-identifier col-name)
                       (format "%s as %s"
                               (escape-identifier table col-name)
                               (escape-identifier col-name))])

                    col-exprs
                    (concat set-cols keep-cols)]
                (jdbc/execute!
                 tx
                 [(format
                   "INSERT INTO %s (%s) SELECT %s FROM %s INNER JOIN %s ON %s = %s WHERE NOT (__singular)"
                   (escape-identifier table)
                   (string/join "," (map first col-exprs))
                   (string/join ", " (map second col-exprs))
                   
                   (escape-identifier table)
                   (escape-identifier temp-table)
                   (escape-identifier table "rowid")
                   (escape-identifier temp-table rowid-col))])

                (when (= method :outer-join)
                  (jdbc/execute!
                   tx
                   [(format
                     "INSERT INTO %s (%s) SELECT %s FROM %s WHERE %s IS NULL OR %s NOT IN (SELECT rowid FROM %s)"
                     (escape-identifier table)
                     (string/join "," (map first set-cols))
                     ;; we don't need case when here
                     (string/join ", " (map first set-cols))
                     (escape-identifier temp-table)
                     (escape-identifier temp-table rowid-col)
                     (escape-identifier temp-table rowid-col)
                     (escape-identifier table))])))

              ;; delete original rows after they have been multiplied up
              (jdbc/execute!
               tx [(format "DELETE FROM %s WHERE rowid IN (SELECT %s FROM %s WHERE NOT(__singular))"
                           (escape-identifier table)
                           (escape-identifier rowid-col)
                           (escape-identifier temp-table))]))))
        (finally
          (jdbc/with-transaction [tx conn]
            (jdbc/execute! tx [(format "DROP TABLE \"%s\"" temp-table)])))))))

