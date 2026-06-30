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

  This namespace talks to SQLite directly (via the xerial sqlite-jdbc
  driver and next.jdbc) and implements the GeoPackage spec itself. 
  The geometry blob codec lives in
  geometry.gpkg.geom and CRS handling in geometry.gpkg.crs.
  "

  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.set :as set]
            [geometry.core :as g]
            [geometry.feature :as f]
            [geometry.gpkg.encode :as geom]
            [geometry.crs :as crs]
            [next.jdbc :as jdbc])
  (:import [org.locationtech.jts.geom Geometry Envelope GeometryFactory]
           [org.locationtech.jts.io WKBReader]
           [java.lang.reflect Method]
           [org.sqlite
            Function
            SQLiteConfig
            SQLiteConfig$JournalMode
            SQLiteConfig$Pragma
            SQLiteConfig$TransactionMode]))

(defn- set-read-uncommitted
  "There is a speling mistak in the SQLite java API, which got corrected
  at some point. This makes us forward compatible for consumers that
  overide the dependency on the sqlite driver"
  [config]
  (try (.setReadUncommitted config true)
       (catch Exception e
         (.setReadUncommited config true))))

(def ^:private ^SQLiteConfig sqlite-config
  "SQLite config, optimised for write speed."
  (doto (SQLiteConfig.)
    (.setJournalMode SQLiteConfig$JournalMode/WAL)
    (.setPragma SQLiteConfig$Pragma/SYNCHRONOUS "OFF")
    (.setTransactionMode SQLiteConfig$TransactionMode/DEFERRED)
    (set-read-uncommitted)
    (.setPragma SQLiteConfig$Pragma/MMAP_SIZE
                (str (* 1024 1024 1024)) ;; 1G
                )))

;; ---------------------------------------------------------------------------
;; GeoPackage ST_* SQL functions.
;;
;; The rtree spatial-index triggers (see `rtree-trigger-sqls`) call
;; ST_MinX/ST_MaxX/ST_MinY/ST_MaxY/ST_IsEmpty on geometry blobs. We
;; register Clojure implementations on the connection so the triggers
;; maintain the index. org.sqlite.Function's value_blob
;; / result methods are protected, so we call them by reflection.

(defn- ^Method declared-method [^String name & param-types]
  (doto (.getDeclaredMethod Function name (into-array Class param-types))
    (.setAccessible true)))

(def ^:private ^Method m-value-blob   (declared-method "value_blob" Integer/TYPE))
(def ^:private ^Method m-result-double (declared-method "result" Double/TYPE))
(def ^:private ^Method m-result-int    (declared-method "result" Integer/TYPE))
(def ^:private ^Method m-result-null   (declared-method "result"))

(def ^:private ^WKBReader st-reader
  "WKBReader for the ST_* functions; only used to recover envelopes, so
   the default factory is fine. SQLite invokes functions single-threaded
   on the connection, so sharing one reader is safe."
  (geom/reader g/*factory*))

(defn- envelope-function
  "A scalar SQL Function returning (f envelope) as a double, where the
   first argument is a GeoPackage geometry blob."
  [f]
  (proxy [Function] []
    (xFunc []
      (let [blob (.invoke m-value-blob this (object-array [(int 0)]))]
        (if (nil? blob)
          (.invoke m-result-null this (object-array 0))
          (let [^Geometry gm (geom/decode blob st-reader)
                e (.getEnvelopeInternal gm)]
            (.invoke m-result-double this (object-array [(double (f e))]))))))))

(defn- is-empty-function []
  (proxy [Function] []
    (xFunc []
      (let [blob (.invoke m-value-blob this (object-array [(int 0)]))]
        (if (nil? blob)
          (.invoke m-result-int this (object-array [(int 1)]))
          (let [^Geometry gm (geom/decode blob st-reader)]
            (.invoke m-result-int this
                     (object-array [(int (if (.isEmpty gm) 1 0))]))))))))

(defn- register-gpkg-functions!
  "Register the GeoPackage ST_* helper functions on `conn` so rtree
   triggers work."
  [^java.sql.Connection conn]
  (Function/create conn "ST_MinX" (envelope-function (fn [^Envelope e] (.getMinX e))))
  (Function/create conn "ST_MaxX" (envelope-function (fn [^Envelope e] (.getMaxX e))))
  (Function/create conn "ST_MinY" (envelope-function (fn [^Envelope e] (.getMinY e))))
  (Function/create conn "ST_MaxY" (envelope-function (fn [^Envelope e] (.getMaxY e))))
  (Function/create conn "ST_IsEmpty" (is-empty-function)))

;; ---------------------------------------------------------------------------
;; Connections

(defn- open-sqlite
  "Open a writable JDBC connection to `file` with the ST_* functions
   registered. Does not bootstrap the GeoPackage metadata tables (see
   `bootstrap-gpkg!`); registering functions has no on-disk effect, so
   this is safe to use for queries too."
  ^java.sql.Connection [file]
  (let [conn (org.sqlite.JDBC/createConnection
              (format "jdbc:sqlite:%s" (.getCanonicalPath (io/as-file file)))
              (.toProperties sqlite-config))]
    (register-gpkg-functions! conn)
    conn))

(defn- read-datasource [file]
  (jdbc/get-datasource
   (format "jdbc:sqlite:file:%s?mode=ro&immutable=1"
           (.getCanonicalPath (io/as-file file)))))

(defn- sqlite-query!
  "Run a read query against `file` over a read-only connection."
  [file query-params]
  (with-open [conn (jdbc/get-connection (read-datasource file))]
    (jdbc/execute! conn query-params)))

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

;; ---------------------------------------------------------------------------
;; GeoPackage metadata bootstrap

(def ^:private gpkg-metadata-ddl
  ["CREATE TABLE IF NOT EXISTS gpkg_spatial_ref_sys (
      srs_name TEXT NOT NULL,
      srs_id INTEGER NOT NULL PRIMARY KEY,
      organization TEXT NOT NULL,
      organization_coordsys_id INTEGER NOT NULL,
      definition TEXT NOT NULL,
      description TEXT)"
   "CREATE TABLE IF NOT EXISTS gpkg_contents (
      table_name TEXT NOT NULL PRIMARY KEY,
      data_type TEXT NOT NULL,
      identifier TEXT UNIQUE,
      description TEXT DEFAULT '',
      last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      min_x DOUBLE,
      min_y DOUBLE,
      max_x DOUBLE,
      max_y DOUBLE,
      srs_id INTEGER,
      CONSTRAINT fk_gc_r_srs_id FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id))"
   "CREATE TABLE IF NOT EXISTS gpkg_geometry_columns (
      table_name TEXT NOT NULL,
      column_name TEXT NOT NULL,
      geometry_type_name TEXT NOT NULL,
      srs_id INTEGER NOT NULL,
      z TINYINT NOT NULL,
      m TINYINT NOT NULL,
      CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name),
      CONSTRAINT uk_gc_table_name UNIQUE (table_name),
      CONSTRAINT fk_gc_tn FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name),
      CONSTRAINT fk_gc_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys (srs_id))"
   "CREATE TABLE IF NOT EXISTS gpkg_extensions (
      table_name TEXT,
      column_name TEXT,
      extension_name TEXT NOT NULL,
      definition TEXT NOT NULL,
      scope TEXT NOT NULL,
      CONSTRAINT ge_tce UNIQUE (table_name, column_name, extension_name))"])

(defn- ensure-srs!
  "Ensure a gpkg_spatial_ref_sys row exists for EPSG `srid`."
  [tx srid]
  (let [{:keys [srs_name organization organization_coordsys_id definition description]}
        (crs/srs-row srid)]
    (jdbc/execute!
     tx
     ["INSERT OR IGNORE INTO gpkg_spatial_ref_sys
        (srs_name, srs_id, organization, organization_coordsys_id, definition, description)
        VALUES (?,?,?,?,?,?)"
      srs_name (int srid) organization (int organization_coordsys_id) definition description])))

(defn- bootstrap-gpkg!
  "Create the GeoPackage metadata tables (if absent) and the mandatory
   default spatial reference systems on `conn`."
  [^java.sql.Connection conn]
  (jdbc/with-transaction [tx conn]
    (.execute (.createStatement conn) "PRAGMA application_id = 1196444487")
    (.execute (.createStatement conn) "PRAGMA user_version = 10200")
    (doseq [ddl gpkg-metadata-ddl]
      (jdbc/execute! tx [ddl]))
    (jdbc/execute!
     tx
     ["INSERT OR IGNORE INTO gpkg_spatial_ref_sys
        (srs_name, srs_id, organization, organization_coordsys_id, definition, description)
        VALUES (?,?,?,?,?,?)"
      "Undefined cartesian SRS" (int -1) "NONE" (int -1) "undefined"
      "undefined cartesian coordinate reference system"])
    (jdbc/execute!
     tx
     ["INSERT OR IGNORE INTO gpkg_spatial_ref_sys
        (srs_name, srs_id, organization, organization_coordsys_id, definition, description)
        VALUES (?,?,?,?,?,?)"
      "Undefined geographic SRS" (int 0) "NONE" (int 0) "undefined"
      "undefined geographic coordinate reference system"])
    (ensure-srs! tx 4326)))

;; ---------------------------------------------------------------------------
;; Metadata queries

(defn table-names
  "Get the tables present in the geopackage or sqlite database, as a
   set of strings."
  [file & {:keys [spatial-only? include-system?]
           :or {spatial-only? false include-system? false}}]
  (if spatial-only?
    (->> (sqlite-query!
          file ["SELECT table_name FROM gpkg_contents WHERE data_type = 'features'"])
         (map :gpkg_contents/table_name)
         (set))
    (->> [(if include-system?
            "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
            "SELECT name FROM sqlite_master WHERE type IN ('table','view')
                   AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'gpkg_%'
                   AND name NOT LIKE 'rtree_%'")]
         (sqlite-query! file)
         (map :sqlite_master/name)
         (set))))

(defn column-names [file table]
  (->> (sqlite-query!
        file
        [(format "PRAGMA table_info(%s)" (escape-identifier table))])
       (map :name)
       (set)))

(defn geometry-column
  "Get the geometry column information for `table`; geopackage spec
  requires that a table only has one geometry column, so this is
  generally safe to use as 'the geometry'.

  When reading with `open`, all geometries are put into the :geometry
  part of a Feature record, so we lose the identity of the geometry
  column; sometimes it is useful to know what the geometry column is
  called in a table (eg. for `amend` to update it)."
  [file table]
  (some-> (sqlite-query!
           file
           ["SELECT column_name \"column-name\", geometry_type_name \"geometry-type\", srs_id srid FROM gpkg_geometry_columns WHERE table_name = ?" table])
          (first)
          (update-keys (comp keyword name))
          (update :geometry-type
                  #(case %
                     "GEOMETRY" :geometry
                     "GEOMETRYCOLLECTION" :geometry-collection
                     "POINT" :point
                     "LINESTRING" :line-string
                     "POLYGON" :polygon
                     "MULTIPOINT" :multi-point
                     "MULTILINESTRING" :multi-line-string
                     "MULTIPOLYGON" :multi-polygon
                     %))))

(defn- geometry-columns-map
  "Map of table-name -> {:column geom-col-name :srid srs-id} for every
   spatial table in `file`. Empty if the file has no
   gpkg_geometry_columns table (i.e. plain sqlite)."
  [file]
  (try
    (into {}
          (for [r (sqlite-query!
                   file ["SELECT table_name, column_name, srs_id FROM gpkg_geometry_columns"])]
            [(:gpkg_geometry_columns/table_name r)
             {:column (:gpkg_geometry_columns/column_name r)
              :srid (:gpkg_geometry_columns/srs_id r)}]))
    (catch Exception _ {})))

(defn- primary-key-column
  "Name of `table`'s primary-key column, or nil. For GeoPackage feature
   tables this is the integer feature id (fid), which is reported
   separately as a rowid rather than as an attribute."
  [file table]
  (->> (sqlite-query! file [(format "PRAGMA table_info(%s)" (escape-identifier table))])
       (filter #(pos? (long (:pk %))))
       (first)
       (:name)))

(defn- ->srid
  "Coerce a CRS reference (an integer EPSG code, or a string like
   \"EPSG:4326\") into an integer srid."
  [x]
  (cond (integer? x) x
        (string? x) (Integer/parseInt (last (string/split x #":")))
        :else (throw (IllegalArgumentException. (str "Unknown type of CRS " x)))))

;; ---------------------------------------------------------------------------
;; Reading

(defn- table-iterator
  "A Closeable Iterator over Features for `table` in `file`.

   `geom-col`/`srid` describe the geometry column (nil for non-spatial
   tables). Geometry blobs are decoded with a reader bound to `factory`,
   and reprojected to `to-crs` if given.

   Internal implementation detail of gpkg/open."
  [file table geom-col srid key-transform fetch-rowid to-crs ^GeometryFactory factory]
  (let [rdr       (geom/reader factory)
        to-srid   (when to-crs (->srid to-crs))
        transform (when (and to-srid srid) (crs/transform srid to-srid))
        eff-srid  (or to-srid srid)
        crs-str   (when eff-srid (str "EPSG:" eff-srid))
        ;; on spatial tables the integer primary key is the feature id,
        ;; which we expose as a rowid rather than as a column attribute
        pk-col    (when geom-col (primary-key-column file table))

        conn ^java.sql.Connection (jdbc/get-connection (read-datasource file))
        stmt ^java.sql.Statement  (.createStatement conn)
        rs   ^java.sql.ResultSet  (.executeQuery
                                   stmt
                                   (if fetch-rowid
                                     (format "SELECT rowid, * FROM \"%s\"" table)
                                     (format "SELECT * FROM \"%s\"" table)))
        md   ^java.sql.ResultSetMetaData (.getMetaData rs)
        cols (vec (for [i (range (if fetch-rowid 2 1) (inc (.getColumnCount md)))
                        :let [col-name (.getColumnName md i)
                              geometry? (and geom-col (= col-name geom-col))
                              col-key (if geometry? :geometry (key-transform col-name))]
                        :when (and col-key (not (and pk-col (= col-name pk-col))))
                        :let [col-type (.getColumnTypeName md i)]]
                    [i
                     col-key
                     (cond
                       geometry?
                       (fn [v] (when v
                                 (let [gm (geom/decode v rdr)]
                                   (if transform
                                     (crs/reproject gm transform to-srid)
                                     gm))))
                       (= "BOOLEAN" col-type) {0 false 1 true}
                       :else identity)]))
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
           (fn [a [^int i n t]] (assoc! a n (t (.getObject rs i))))
           (transient (cond-> {:table table :crs crs-str}
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

  (let [geom-cols (geometry-columns-map gpkg)

        tables (if table-name
                 (let [known (table-names gpkg :include-system? true)]
                   (when-not (known table-name)
                     (throw (ex-info "Table not found in geopackage"
                                     {::missing-table table-name
                                      :file gpkg
                                      :table table-name
                                      :tables known})))
                   [table-name])
                 (into [] (table-names gpkg :spatial-only? spatial-only?)))

        state (volatile! {:tables tables
                          :iterator nil
                          :closed false})

        maybe-advance!
        (fn maybe-advance! [{:keys [^java.util.Iterator iterator tables] :as state}]
          (when (:closed state)
            (throw (ex-info "Iterating on a geopackage that has been closed"
                            {:input gpkg :state state})))
          (if (and iterator (.hasNext iterator))
            state ;; just continue with this iterator

            (if (seq tables) ;; there are more tables, start next table
              (let [[^String table & more-tables] tables
                    {gcol :column gsrid :srid} (get geom-cols table)]
                (when iterator (.close ^java.io.Closeable iterator))
                (let [next-state
                      (assoc state
                             :tables more-tables
                             :iterator (table-iterator gpkg table gcol gsrid
                                                       key-transform rowids?
                                                       to-crs geometry-factory))]
                  ;; deal with empty tables: an iterator that has nothing in
                  ;; it must be skipped so the outer iterator doesn't stop early
                  (if (.hasNext ^java.util.Iterator (:iterator next-state))
                    next-state
                    (maybe-advance! next-state))))

              ;; reached the end
              (do
                (when iterator (.close ^java.io.Closeable iterator))
                {:tables nil :iterator nil :closed false}))))]

    (reify
      java.lang.AutoCloseable
      (close [_]
        (vswap! state (fn [state]
                        (when-let [^java.io.Closeable i (:iterator state)] (.close i))
                        (assoc state :iterator nil :closed true))))

      java.util.Iterator
      (next [_]
        (let [state (vswap! state maybe-advance!)]
          (when-let [^java.util.Iterator iterator (:iterator state)]
            (.next iterator))))

      (hasNext [_]
        (let [state (vswap! state maybe-advance!)]
          (if-let [^java.util.Iterator iterator (:iterator state)]
            (.hasNext iterator)
            false))))))

;; ---------------------------------------------------------------------------
;; Schema inference & DDL

(def ^:private geometry-keys
  #{:geometry :geometry-collection :line-string :multi-line-string
    :multi-point :multi-polygon :point :polygon})

(def ^:private geometry-type-names
  {:geometry "GEOMETRY"
   :geometry-collection "GEOMETRYCOLLECTION"
   :point "POINT"
   :line-string "LINESTRING"
   :polygon "POLYGON"
   :multi-point "MULTIPOINT"
   :multi-line-string "MULTILINESTRING"
   :multi-polygon "MULTIPOLYGON"})

(defn- kv-type [k v]
  [(name k)
   (cond->
       {:accessor (if (keyword? k) k #(get % k))
        :type
        (if (instance? Geometry v)
          :geometry
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
            ":String"))}
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
             {:type :geometry
              :srid  (g/srid feature)
              :accessor g/geometry}]]
           (for [k (sort-by str keys)
                 :when (and (not= k :geometry)
                            (not= k :table)
                            (not= k :crs))]
             (kv-type k (get feature k)))))

        (satisfies? g/HasGeometry feature)
        [["geometry"
          {:type :geometry
           :accessor g/geometry
           :srid (g/srid feature)}]]

        (map? feature)
        (for [[k v] (sort-by (comp str first) feature)] (kv-type k v))

        :else
        (throw (ex-info "Unable to infer schema for value" {:value feature}))))

(defn- spec-geom-field
  "Get the spec for the (first) geometry field from a schema."
  [spec]
  (first (filter
          (fn [[_ {:keys [type]}]] (contains? geometry-keys type))
          spec)))

(let [integral #{:int :integer :short :long Integer Short Long
                 (.getCanonicalName Integer)
                 (.getCanonicalName Short)
                 (.getCanonicalName Long)}

      real #{:double :float :real Double Float
             (.getCanonicalName Double)
             (.getCanonicalName Float)}

      bool #{:boolean Boolean (.getCanonicalName Boolean)}]
  (defn- create-table-ddl
    "Converts the `spec` for a table into a map containing

  {:create-table <ddl-sql>
   :insert <insert-sql>
   :has-integer-primary-key bool}"
    [table-name spec]
    (let [col-types
          (for [[col {col-type :type
                      primary-key :primary-key
                      foreign-key :foreign-key
                      not-null :not-null}] spec
                :let [on-delete (:on-delete foreign-key)
                      on-update (:on-update foreign-key)
                      foreign-key (if (map? foreign-key)
                                    (:column foreign-key)
                                    foreign-key)]]
            (-> (escape-identifier col)
                (str " ")
                (str (cond (integral col-type) "INTEGER"
                           (real col-type) "REAL"
                           (bool col-type) "BOOLEAN"
                           (geometry-keys col-type) (-> col-type
                                                        (name)
                                                        (string/replace #"-" "")
                                                        (string/upper-case))
                           :else "TEXT"))
                (cond-> not-null (str " NOT NULL")
                        primary-key (str " PRIMARY KEY")
                        (= :auto-increment primary-key) (str " AUTOINCREMENT")

                        foreign-key (str (format " REFERENCES %s (%s)"
                                                 (escape-identifier (namespace foreign-key))
                                                 (escape-identifier (name foreign-key))))

                        on-delete (str " ON DELETE "
                                       (string/upper-case
                                        (string/replace (name on-delete) #"-" " ")))

                        on-update (str " ON UPDATE "
                                       (string/upper-case
                                        (string/replace (name on-update) #"-" " "))))))

          cols (mapv (comp name first) spec)]

      {:create-table
       (format "CREATE TABLE IF NOT EXISTS %s (%s);" (escape-identifier table-name) (string/join ", " col-types))

       :insert
       (format "INSERT INTO %s (%s) values (%s)"
               (escape-identifier table-name)
               (string/join ", " (map escape-identifier cols))
               (string/join ", " (repeat (count cols) "?")))

       :has-integer-primary-key
       (some (fn [{:keys [primary-key type]}]
               (and (integral type) (boolean primary-key)))
             (map second spec))})))

;; ---------------------------------------------------------------------------
;; Spatial table registration & index

(defn- register-feature-table!
  "Register `table` as a features table in gpkg_contents and
   gpkg_geometry_columns."
  [tx table geom-col geom-type srid]
  (jdbc/execute!
   tx
   ["INSERT OR IGNORE INTO gpkg_contents
      (table_name, data_type, identifier, srs_id, min_x, min_y, max_x, max_y)
      VALUES (?,?,?,?,?,?,?,?)"
    table "features" table (int srid) 0.0 0.0 0.0 0.0])
  (jdbc/execute!
   tx
   ["INSERT OR IGNORE INTO gpkg_geometry_columns
      (table_name, column_name, geometry_type_name, srs_id, z, m)
      VALUES (?,?,?,?,0,0)"
    table geom-col geom-type (int srid)]))

(defn- rtree-trigger-sqls
  "The six rtree-maintenance triggers from the GeoPackage spec, for
   `table`.`geom-col` indexed by virtual table `rtree`, keyed on the
   integer primary key `pk`."
  [table geom-col rtree pk]
  (let [t (escape-identifier table)
        g (escape-identifier geom-col)
        r (escape-identifier rtree)
        k (escape-identifier pk)
        tn (fn [suf] (escape-identifier (str rtree suf)))
        ins-vals (format "NEW.%s, ST_MinX(NEW.%s), ST_MaxX(NEW.%s), ST_MinY(NEW.%s), ST_MaxY(NEW.%s)"
                         k g g g g)]
    [(format "CREATE TRIGGER IF NOT EXISTS %s AFTER INSERT ON %s WHEN (NEW.%s NOT NULL AND NOT ST_IsEmpty(NEW.%s)) BEGIN INSERT OR REPLACE INTO %s VALUES (%s); END"
             (tn "_insert") t g g r ins-vals)
     (format "CREATE TRIGGER IF NOT EXISTS %s AFTER UPDATE OF %s ON %s WHEN OLD.%s = NEW.%s AND (NEW.%s NOTNULL AND NOT ST_IsEmpty(NEW.%s)) BEGIN INSERT OR REPLACE INTO %s VALUES (%s); END"
             (tn "_update1") g t k k g g r ins-vals)
     (format "CREATE TRIGGER IF NOT EXISTS %s AFTER UPDATE OF %s ON %s WHEN OLD.%s = NEW.%s AND (NEW.%s ISNULL OR ST_IsEmpty(NEW.%s)) BEGIN DELETE FROM %s WHERE id = OLD.%s; END"
             (tn "_update2") g t k k g g r k)
     (format "CREATE TRIGGER IF NOT EXISTS %s AFTER UPDATE ON %s WHEN OLD.%s != NEW.%s AND (NEW.%s NOTNULL AND NOT ST_IsEmpty(NEW.%s)) BEGIN DELETE FROM %s WHERE id = OLD.%s; INSERT OR REPLACE INTO %s VALUES (%s); END"
             (tn "_update3") t k k g g r k r ins-vals)
     (format "CREATE TRIGGER IF NOT EXISTS %s AFTER UPDATE ON %s WHEN OLD.%s != NEW.%s AND (NEW.%s ISNULL OR ST_IsEmpty(NEW.%s)) BEGIN DELETE FROM %s WHERE id IN (OLD.%s, NEW.%s); END"
             (tn "_update4") t k k g g r k k)
     (format "CREATE TRIGGER IF NOT EXISTS %s AFTER DELETE ON %s WHEN OLD.%s NOT NULL BEGIN DELETE FROM %s WHERE id = OLD.%s; END"
             (tn "_delete") t g r k)]))

(defn- create-spatial-index!
  "Create an rtree spatial index on `table`.`geom-col`, keyed on `pk`,
   and register it in gpkg_extensions, with the spec triggers that keep
   it up to date."
  [tx table geom-col pk]
  (let [rtree (str "rtree_" table "_" geom-col)]
    (jdbc/execute!
     tx [(format "CREATE VIRTUAL TABLE IF NOT EXISTS %s USING rtree(\"id\", \"minx\", \"maxx\", \"miny\", \"maxy\")"
                 (escape-identifier rtree))])
    (jdbc/execute!
     tx ["INSERT OR IGNORE INTO gpkg_extensions
           (table_name, column_name, extension_name, definition, scope)
           VALUES (?,?,?,?,?)"
         table geom-col "gpkg_rtree_index"
         "http://www.geopackage.org/spec120/#extension_rtree" "write-only"])
    (doseq [s (rtree-trigger-sqls table geom-col rtree pk)]
      (jdbc/execute! tx [s]))))

(defn- set-layer-extent!
  "Update the extent (if not nil) for `table` in gpkg_contents on `tx`."
  [tx table ^Envelope extent]
  (when (and extent (not (.isNull extent)))
    (jdbc/execute!
     tx
     ["UPDATE gpkg_contents SET min_x = ?, min_y = ?, max_x = ?, max_y = ? WHERE table_name = ?"
      (.getMinX extent) (.getMinY extent) (.getMaxX extent) (.getMaxY extent) table])))

(defn drop-table
  "Drop `table` within geopackage `file`, cleaning up its GeoPackage
   metadata (gpkg_contents / gpkg_geometry_columns / gpkg_extensions)
   and any rtree spatial index and triggers."
  [file table-name]
  (with-open [conn (open-sqlite file)]
    (jdbc/with-transaction [tx conn]
      (doseq [{c :gpkg_geometry_columns/column_name}
              (try (jdbc/execute!
                    tx ["SELECT column_name FROM gpkg_geometry_columns WHERE table_name = ?" table-name])
                   (catch Exception _ nil))]
        (let [rtree (str "rtree_" table-name "_" c)]
          (doseq [suf ["_insert" "_update1" "_update2" "_update3" "_update4" "_delete"]]
            (jdbc/execute! tx [(format "DROP TRIGGER IF EXISTS %s" (escape-identifier (str rtree suf)))]))
          (jdbc/execute! tx [(format "DROP TABLE IF EXISTS %s" (escape-identifier rtree))])))
      (doseq [meta-table ["gpkg_geometry_columns" "gpkg_extensions" "gpkg_contents"]]
        (try (jdbc/execute!
              tx [(format "DELETE FROM %s WHERE table_name = ?" meta-table) table-name])
             (catch Exception _)))
      (jdbc/execute! tx [(format "DROP TABLE IF EXISTS %s" (escape-identifier table-name))]))))

;; ---------------------------------------------------------------------------
;; Writing

(defn- expand-extent
  "Grow `extent` (an Envelope, or nil) to include `geom`'s envelope."
  ^Envelope [^Envelope extent ^Geometry geom]
  (if (nil? geom)
    extent
    (let [e (.getEnvelopeInternal geom)]
      (cond
        (.isNull e) extent
        (nil? extent) (Envelope. e)
        :else (doto extent (.expandToInclude e))))))

(defn- insert-features!
  "Insert `features` into `table` on `conn` using `insert-sql` (whose
   columns match `spec`, in order). Encodes geometry columns to
   GeoPackage blobs (with `srid`), maps booleans to 0/1, and returns the
   accumulated Envelope of all geometries.

   Pulls features one at a time off an Iterator (rather than holding the
   seq head) so that a lazily-generated input seq can be GC'd as we go."
  ^Envelope [^java.sql.Connection conn insert-sql spec features srid batch-insert-size]
  (let [getters (mapv (fn [[k v]] (or (:accessor v) #(get % k))) spec)
        kinds   (mapv (fn [[_ {:keys [type]}]]
                        (cond (geometry-keys type) :geom
                              (or (= :boolean type) (= Boolean type)
                                  (= "java.lang.Boolean" type)) :bool
                              :else :other))
                      spec)
        geom-idx (first (keep-indexed (fn [i k] (when (= :geom k) i)) kinds))
        n-cols (count spec)
        ^Iterable feature-seq (or features [])
        iter ^java.util.Iterator (.iterator feature-seq)]
    (.setAutoCommit conn false)
    (let [extent
          (with-open [ps (.prepareStatement conn insert-sql)]
            (loop [extent nil
                   pending 0]
              (if (.hasNext iter)
                (let [feature (.next iter)
                      vals (object-array n-cols)]
                  (dotimes [i n-cols]
                    (aset vals i ((getters i) feature)))
                  (dotimes [i n-cols]
                    (let [v (aget vals i)
                          col (inc i)]
                      (cond
                        (nil? v) (.setObject ps col nil)
                        (= :geom (kinds i)) (.setBytes ps col (geom/encode v srid))
                        (= :bool (kinds i)) (.setInt ps col (if v 1 0))
                        :else (.setObject ps col v))))
                  (.addBatch ps)
                  (let [extent (expand-extent extent (when geom-idx (aget vals geom-idx)))
                        pending (inc pending)]
                    (if (>= pending batch-insert-size)
                      (do (.executeBatch ps) (recur extent 0))
                      (recur extent pending))))
                (do (when (pos? pending) (.executeBatch ps))
                    extent))))]
      (.commit conn)
      (.setAutoCommit conn true)
      extent)))

(defn write
  "Write the given sequence of `features` into a geopackage at `file`
  in a table called `table-name`

  The `schema` argument is good to supply; without it the schema will be
  inferred which may not be what you want.

  the form of `schema` is a map or a seq of tuples, like

  [[column-1-name column-1-details]
   [column-2-name column-2-details]]

  Column names can be a keyword or a string.

  Column details are a map like

  {:type        column-type
   :accessor    value-accessor
   :srid        1234
   :not-null    true|false
   :primary-key true | :auto-increment
   :foreign-key :table/column | {:column :table/column
                                 :on-delete :set-null
                                 :on-update :cascade}
   }

  only :type is required, and :srid if :type is a geometry
  column types are things like :point, :multi-polygon, :integer etc.

  if :accessor is not provided, the column name is used to get values
  from each row for this column. Otherwise accessor is invoked on each
  row.

  Because of how sqlite works, these are only guaranteed to take effect
  if the target table doesn't exist

  You can use `:if-exists` to guarantee this; options are

  - :append (the default) will append to the table if possible, ignoring
    the schema if the table is present
  - :delete-rows will DELETE FROM the table if present, ignoring schema.
    this might be what you want if you have indexes / triggers to preserve
  - :drop-table will drop and recreate the table from the schema

  `:add-spatial-index` will ensure a spatial index exists, if it is a
  spatial table. If non spatial, no effect.
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
     (drop-table file table-name)

     (= :delete-rows if-exists)
     (with-open [conn (open-sqlite file)]
       (try
         (jdbc/with-transaction [tx conn]
           (jdbc/execute!
            tx
            [(format "DELETE FROM %s" (escape-identifier table-name))]))
         (catch Exception _))))

   (let [spec (vec (or schema (infer-spec (first features))))
         [geom-field {:keys [srid type] :or {srid 27700}}] (spec-geom-field spec)
         geom-col (when geom-field (name geom-field))
         geom-type (when geom-field (geometry-type-names type "GEOMETRY"))
         ddl (create-table-ddl
              table-name
              (cond-> spec
                geom-field
                (conj [:fid {:type :integer
                             :primary-key :auto-increment
                             :not-null true}])))]
     (with-open [conn (open-sqlite file)]
       (bootstrap-gpkg! conn)
       (if geom-field
         ;; spatial data:
         (let [insert-sql (format "INSERT INTO %s (%s) VALUES (%s)"
                                  (escape-identifier table-name)
                                  (string/join ", " (map (comp escape-identifier name first) spec))
                                  (string/join ", " (repeat (count spec) "?")))]
           (jdbc/with-transaction [tx conn]
             (ensure-srs! tx srid)
             (jdbc/execute! tx [(:create-table ddl)])
             (register-feature-table! tx table-name geom-col geom-type srid)
             (when add-spatial-index
               (create-spatial-index! tx table-name geom-col "fid")))

           (let [extent (insert-features! conn insert-sql spec features srid batch-insert-size)]
             (jdbc/with-transaction [tx conn]
               (set-layer-extent! tx table-name extent))))

         ;; non-spatial data:
         (jdbc/with-transaction [tx conn]
           (jdbc/execute! tx [(:create-table ddl)])
           (when (:has-integer-primary-key ddl)
             (jdbc/execute!
              tx
              ["INSERT INTO gpkg_contents (table_name, data_type, identifier) VALUES (?,?,?) ON CONFLICT DO NOTHING"
               table-name "attributes" table-name]))
           (jdbc/execute-batch!
            tx
            (:insert ddl)
            (for [feature features]
              (for [[col {accessor :accessor}] spec]
                (let [v (if (nil? accessor) (get feature col) (accessor feature))]
                  (if (instance? Boolean v) (if v 1 0) v))))
            {:batch-size batch-insert-size})))
       nil))))

(defn amend
  "Update existing rows in `table` within `gpkg`. `values` are the new values to write in.

  If you know there is a PK you can use you can supply :primary-key [\"col name\" accessor]

  This is probaly safer than the fallback which is to use rowid.

  `gpkg/read` above will extract rowid if you ask for it, and you can
  join back, but rowid can change during a transaction if there are
  gaps in the rowid sequence and the table is vacuumed.

  The expected usage is that you pull rows out with `read` including
  rowid, update / create some columns, and then call `amend` with a
  schema for the columns to be updated.

  `if-exists` can be `:preserve`, `:set-null` or `:drop-column`.

  `:drop-column` is safest, since you don't need to be sure the types
  align. It requires a sqlite driver new enough to support DROP COLUMN
  in ALTER TABLE.

  `method` can be `:update-set`, `:left-join`, `:outer-join`

  - `:update-set` works as UPDATE table SET cols = vals WHERE table-rowid = input.rowid

    unmatched rows are affected according to `if-exists`
    the inputs must be distinct on ::rowid
  - `:left-join` works as CREATE table AS (SELECT cols FROM old_table LEFT JOIN input)

    unmatched rows are affected according to `if-exists`
    if inputs are not distinct on ::rowid, new rows are created. old rowids are broken
    so any internal joins / triggers etc related to these will be damaged.
  - `:outer-join` does `:left-join` and then inserts unmatched rows
  - `:right-join` does `:outer-join` and then deletes rows that are unmatched in the update

  The actual implementation here is:
  1. insert data into a temp table
  2. (if needed) zap the existing columns
  3. add any missing columns
  4. update the temp table to mark repeat rowids (for left-join type behaviour)
  5. issue UPDATE SET to copy cols for non-repeating rows
  6. issue DELETE for anything not matched, if right-join
  7. issue INSERT INTO (SELECT FROM JOIN) for repeating rows
  8. issue DELETE for original copies of repeating rows

  This routine will at least preserve indexes and FK relationships on the target table.

  It may fail if the result is not valid.
  "
  [gpkg table values
   & {:keys [schema if-exists method primary-key]
      :or {if-exists :set-null method :update-set
           primary-key ["rowid" ::rowid]}}]

  {:pre [(#{:update-set :left-join :right-join :outer-join} method)]}

  (let [[pk-col pk-accessor] primary-key

        escape-identifier (memoize escape-identifier)
        schema (or schema (infer-spec (first values)))
        temp-table "__temp__"
        rowid-col "_original_rowid"

        temp-orig-rowid (escape-identifier temp-table rowid-col)
        table-rowid     (escape-identifier table pk-col)]
    (try
      ;; first write the new columns into our temporary table
      (write
       gpkg temp-table values
       :schema (conj
                schema
                [rowid-col {:type :long :accessor pk-accessor}]))

      (with-open [conn (open-sqlite gpkg)]
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
                     [(format "ALTER TABLE %s DROP COLUMN %s" (escape-identifier table) (escape-identifier col))]))

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

                                temp-orig-rowid ;; group by %s
                                temp-orig-rowid)]) ;; where %s =
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
                      table-rowid temp-orig-rowid
                      (escape-identifier temp-table))])

            (when (= method :right-join)
              ;; delete existing rows that are unmatched by any of the rows in temp table
              (jdbc/execute!
               tx [(format "DELETE FROM %s WHERE NOT EXISTS (SELECT * FROM %s WHERE %s = %s)"
                           (escape-identifier table)
                           (escape-identifier temp-table)
                           temp-orig-rowid
                           table-rowid)]))

            (when (or (= method :left-join) (= method :outer-join) (= method :right-join))
              ;; left join and outer join both need the multiplied up rows inserting
              (let [set-cols ;; all the cols we are setting
                    (for [col src-cols]
                      [(escape-identifier (:name col))
                       (format "(CASE WHEN %s NOT NULL THEN %s ELSE %s END) AS %s"
                               temp-orig-rowid
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
                   table-rowid
                   temp-orig-rowid)])

                (when (or (= method :outer-join) (= method :right-join))
                  (jdbc/execute!
                   tx
                   [(format
                     "INSERT INTO %s (%s) SELECT %s FROM %s WHERE %s IS NULL OR %s NOT IN (SELECT %s FROM %s)"
                     (escape-identifier table)
                     (string/join "," (map first set-cols))
                     ;; we don't need case when here
                     (string/join ", " (map first set-cols))
                     (escape-identifier temp-table)
                     temp-orig-rowid
                     temp-orig-rowid
                     table-rowid
                     (escape-identifier table))])))

              ;; delete original rows after they have been multiplied up
              (jdbc/execute!
               tx [(format "DELETE FROM %s WHERE %s IN (SELECT %s FROM %s WHERE NOT(__singular))"
                           (escape-identifier table)
                           table-rowid
                           temp-orig-rowid
                           (escape-identifier temp-table))])))))
      (finally
        (drop-table gpkg temp-table)))))
