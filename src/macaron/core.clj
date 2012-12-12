(ns macaron.core
  (:use [clojure.tools.logging])
  (:require [clojure.java.jdbc :as sql])
  (:require [clojure.string :as string])
  (:require [clojure.set :as set])
  )

(defmacro with-connection [db & body]
  `(sql/with-connection ~db ~@body)
  )

(defn current-database
  "Returns the currently selected database name"
  []
  (sql/with-query-results rs ["SELECT DATABASE() as db"]
    (:db (first rs)))
  )

(defn add-default-triggers [table]
  (sql/do-commands (str "DROP TRIGGER IF EXISTS before_insert_" table))
  (sql/do-commands (str "CREATE TRIGGER before_insert_" table " BEFORE INSERT ON " table " FOR EACH ROW 
    BEGIN
     IF new.uuid IS NULL THEN 
       SET new.uuid = uuid();
     END IF;

     IF new.dateadded IS NULL THEN
       SET new.dateadded = now();
     END IF;
    END")))

(defn update-entity-with-version [table entity]
  (let [version (:version entity)
        version (if (nil? version) 0 version)
        versionentity (assoc entity :version (+ version 1))
        updateresult (sql/update-values table ["id=? AND version=?" (:id entity) (:version entity)] versionentity)]
    (if (= (first updateresult) 1)
      versionentity
      (not (sql/set-rollback-only))
      )
   )
  )

(defn save-entity [table entity]
  (if (and (contains? entity :id) (:id entity))
    (if (contains? entity :version)
      (update-entity-with-version table entity)
      (do (sql/update-values table ["id=?" (:id entity)] entity) entity))
    (assoc entity :id (:generated_key (sql/insert-record table entity)))
    ))


(defn save-entities [table entities]
  (doall (for [entity entities]
           (save-entity table entity))))

(defn find-by
  "Find an entity by the idcolum provided, so that (find-by :id \"Users\" 1) would find the user with id = 1"
  [idcol table value]
  (sql/with-query-results rs [(format "SELECT * FROM %s WHERE %s = ?" table (sql/as-identifier idcol)) value]
                          (into {} rs))
  )

(defn delete-by
  "Delete an entity using the specified column provided"
  [col table value]
  (debug "Deleting data in table: " table " column: " col " value: " value)
  (sql/delete-rows table [(format "%s = ?" (sql/as-identifier col)) value]))

(comment
  "This behaviour is strongly discouraged, so it's removed for now. You should almost exclusively use named queries even for the simplest queries (outside of 'get-list')"
  (defn find-list
    "Find an entity by the idcolum provided, so that (find-by :groupid \"Users\" 1) would find the users with groupid = 1"
    [idcol table value]
    (sql/with-query-results rs [(format "SELECT * FROM %s WHERE %s = ?" table (sql/as-identifier idcol)) value]
      (apply list rs))
    ))

(defn get-list
  "Get a simple list of entities from the database."
  [table]
  (sql/with-query-results rs [(format "SELECT * FROM %s" table)] (apply list rs)))

(defn find-or-create [idcol table entity]
  {:pre (map? entity)}
  (let [id (get entity idcol :not-found)
        ]
    (cond
     (= :not-found id) (throw (RuntimeException. (str "Id column " idcol " not found on entity " entity)))
     :else
     (let [ent (find-by idcol table id)]
       (if (empty? ent)
         (save-entity table entity)
         ent))) 
    )
  )

(defn filter-existing
  "Filters out existing columns for the update-table function"
  [specs existing]
  (filter (fn [col] (nil? (some #{(sql/as-identifier (first col))} existing))) specs)
  )

(defn table-exists [table-name]
  (not (nil? (sql/with-query-results rs [(str "SHOW TABLES LIKE \"" table-name "\"")] (first rs)))))

(defn index-exists?
  "Check if a named index exists on a table"
  [table-name index-name]
  (sql/with-query-results rs [ (str "SHOW INDEX FROM " table-name " WHERE key_name=?") index-name]
    (not (empty? rs))))

(defn add-index
  "Add an index to a table if it doesn't exist, just silently ignore if it does though"
  ([table-name column-name]
     (add-index table-name column-name [[column-name]]))
  ([table-name index-name columns]
     (add-index table-name index-name "" columns))
  ([table-name index-name index-type columns]
     (let [coldefs (apply str (map sql/as-identifier (apply concat (interpose ", "
                                                                              (map (partial interpose " ") columns)))))
           index-type (sql/as-identifier index-type)
           index-name (sql/as-identifier index-name)
           alterstr (str "ALTER TABLE " table-name " ADD INDEX " index-name " (" coldefs ")")]
       (if (index-exists? table-name index-name)
         false
         (do
           (debug alterstr)
           (sql/do-commands alterstr))
         ))))

(defn fk-exists?
  "Check if a named foreign key exists on a table"
  [table-name fk-name]
  (sql/with-query-results rs ["SELECT * FROM information_schema.KEY_COLUMN_USAGE WHERE referenced_table_name is not null AND table_schema=? AND TABLE_NAME=? AND CONSTRAINT_NAME=?" (current-database) table-name fk-name]
    (not (empty? rs))))

(defn add-foreignkey
  "Adds a foreign key link to a table if it doesn't exist. Ignores silently if it does exist."
  [table-name column-name fk-table-name fk-column-name optionmap]
  (cond
   (nil? fk-table-name) (throw (RuntimeException. (str "No foreign key table for " table-name "." column-name " link")))
   :else
   (let [fk-name (str table-name "_" column-name "_fk")
         reference (str fk-table-name "(" fk-column-name ")")
         on-delete (-> (get optionmap :delete :restrict) name .toUpperCase)
         on-update (-> (get optionmap :update :restrict) name .toUpperCase)
         alterstr (str "ALTER TABLE " table-name " ADD CONSTRAINT " fk-name" FOREIGN KEY " fk-name " (" column-name ") REFERENCES " reference " ON DELETE " on-delete " ON UPDATE " on-update)]
     (if (fk-exists? table-name fk-name)
       false
       (do
         (debug alterstr)
         (sql/do-commands alterstr)))))

  )

(defn check-enum-columns
  "Checking if a column's type is an enum and include it in the returned collection"
  [columns column]
  (if (.startsWith (:type column) "enum")
    (conj columns column)
    columns))

(defn get-enum-specs 
  "Return the enum specs for matching enum fields"
  [specs fields]
  (reduce 
   (fn [allspecs currentspec]
     (if (not (nil? (some #{(name (first currentspec))} fields)))
       (conj allspecs currentspec)
       allspecs))
    (list) 
    specs))

(defn get-existing-enum-types
  "Find the current enum and return its current types"
  [name enums]
  (reduce
   (fn [all current]
     (if (= (:field current) name)
             (conj all (:type current))
             all))
   (list)
   enums))

(defn merge-types
  "Strips out just the names of the enum types from a string with types, commas and spaces"
  [types]
  (reduce
   (fn [all f]
     (if-not (= (.trim f) ",")
       (conj all (str "'" (.trim f) "'"))
       all))
   (hash-set) types))

(defn parse-types
  "Meh function to strip out the types from the spec or column definition string. Skips first 6 letters and the last letter to remove enum( ) "
  [enum]
  (string/split (subs enum 6 (- (.length enum) 1)) #"\'"))

(defn merge-enum-types
  "Merged new and existing enum types together, eg: enum('boy','girl') and ENUM('male', 'female') returns enum('boy', 'girl', 'male', 'female')"
  [new existing]
  (let [existing-enums (merge-types (parse-types existing))
        new-enums (merge-types (parse-types new))
        missing (set/difference new-enums existing-enums)        
        types (set/union new-enums existing-enums)
        fields (apply str (concat (interpose "," types)))]
    (if (> (count missing) 0)      
      (str "enum(" fields ")"))))

(defn merge-enums
  "Merge old enum values into the new version of the enum so that old types are preserved but the new enum types are added
   Eg: existing: {:field gender, :type enum('boy','girl'), :null YES, :key , :default nil, :extra } and new: [:gender ENUM('male', 'female')]"
  [new existing]
  (reduce
   (fn [all n]
     (let [existing-types-as-string (first (get-existing-enum-types (name (first n)) existing))
           merged-types (merge-enum-types (second n) existing-types-as-string)]
       (if-not (nil? merged-types)
         (conj all (vector (first n) merged-types))
         all)))
       {}
       new))

(defn add-enum-name
  "Hack to add the enum's name to its spec in the front so the correct sql will be generated
   Eg: [:gender ENUM('boy', 'girl')] converts to [:gender :gender ENUM('boy', 'girl')]"
  [all current]
  (conj all (into (vector (first current)) current)))

(defn enum-specs-to-string
  "Convert the enum into a sql query string so the query can be run to update all the table's enums"
  [enum-specs existing-enums]
  (debug "Existing enums:" existing-enums "New enum defns:" enum-specs)
  (let [merged-enums (merge-enums enum-specs existing-enums)
        specs (reduce add-enum-name (list) merged-enums)]
    (if (> (count merged-enums) 0)
      (apply str
             (map sql/as-identifier
                  (apply concat
                         (interpose [", CHANGE "]
                                    (map (partial interpose " ") specs)))))
      nil)))

(defn update-table 
  "Adds non-existing columns to a table on the open database connection given a table name and
  specs. This can be used instead of create-table Each spec is either a column spec: a vector containing a column
  name and optionally a type and other constraints, or a table-level
  constraint: a vector containing words that express the constraint.  All words used to
  describe the table may be supplied as strings or keywords."  
  [table-name & specs]
  (def exists (table-exists table-name))
  (let [split-specs (partition-by #(= :table-spec %) specs)
         col-specs (first split-specs)
	       table-spec (first (second (rest split-specs)))
	       table-spec-str (or (and table-spec (str " " table-spec)) "")]
	  ; Create the table with placeholder column (to remove after other columns are added.
	  (if (not exists)
	    (sql/do-commands (str "CREATE TABLE " table-name " (__placeholder__ int) ENGINE=innodb, CHARACTER SET=utf8, COLLATE=utf8_general_ci" table-spec-str)))
	  
	  (sql/with-query-results rs [(str "SHOW COLUMNS FROM " table-name)]
            (let [existing-columns (map #(:field %) rs)
	          remaining-cols (filter-existing specs existing-columns)
		  specs-to-string (fn [specs]
			            (apply str
			              (map sql/as-identifier
			                (apply concat
			                  (interpose [", ADD COLUMN "]
			                    (map (partial interpose " ") remaining-cols))))))
	           query (format "ALTER TABLE %s ADD COLUMN %s %s"
		            (sql/as-identifier table-name)
		            (specs-to-string col-specs)
		            table-spec-str)
                  existing-enums (reduce check-enum-columns (list) rs)
                  existing-enum-names (map #(:field %) existing-enums)
                  enum-col-specs (get-enum-specs col-specs existing-enum-names)
                  enum-sql (enum-specs-to-string enum-col-specs existing-enums)
                  enum-query (format "ALTER TABLE %s CHANGE %s %s"
		            (sql/as-identifier table-name)
		            enum-sql
		            table-spec-str)
                  ]
              (when (> (count remaining-cols) 0)
                     (info "Remaining column" remaining-cols)
                     (info "Running queries:" query)
                     (sql/do-commands query)
                     )

              ; Redefine the existing enum columns
              (when (not (nil? enum-sql))
                     (info "Running enum update queries:" enum-query)
                     (sql/do-commands enum-query))                   
              ))
	     
	  ; Remove placeholder column
	  (if (not exists)
	    (sql/do-commands (str "ALTER TABLE " table-name " DROP COLUMN __placeholder__")))
    )
  )

(def entity-uuid-cachemap (ref {}))

(defn cache [uuid id]
  (dosync
     (ref-set entity-uuid-cachemap (assoc @entity-uuid-cachemap uuid id))
     id))

(defn get-cached-entity-id [table uuid]
  (if (or (= uuid "0") (= uuid ""))
    0
    (let [id (@entity-uuid-cachemap uuid :notfound)]
      (if (= :notfound id)
        (let [entity (find-by :uuid table uuid)]
          (cache uuid (:id entity)))
        id
        )
      )
    ))

(defn cache-all [table]
  (dosync
    (sql/with-query-results rs [(format "SELECT id, uuid FROM %s" table)] 
      (ref-set entity-uuid-cachemap (merge @entity-uuid-cachemap (apply merge (map #(hash-map (:uuid %) (:id %)) rs))))
      )))

(defn cache-clear [table]
  (dosync
   (sql/with-query-results rs [(format "SELECT uuid FROM %s" table)]
     (ref-set entity-uuid-cachemap (apply dissoc @entity-uuid-cachemap (map #(:uuid %) rs)))
     )))



(defonce entitydefs (atom {}))

(defn get-named-section [nm opts]
  (drop 1 (first (filter #(= (first %) (symbol nm)) opts)))
  )

(defn has-named-section [nm opts]
  (not-empty (first (filter #(= (first %) (symbol nm)) opts)))
  )

(declare defmtmlink)

(defn new-entity
  "Create a new entity hashmap, stripping out all keys not present"
  [fieldnames valuemap]
  (select-keys valuemap fieldnames))

(def keyword-match #":([A-Za-z0-9+*_-]+)")

(defn get-query-paramnames
  "Gets the parameters of a query as names, ordered by match"
  [qry]
  (map #(second %) (re-seq keyword-match qry)))

(defn get-query-paramkeys
  "Gets the parameters of a query as keys, ordered by match"
  [qry]
  (map #(keyword %) (get-query-paramnames qry)))

(defn get-fndef
  "Generates a function for an entity's named-query"
  [entname qry]
  (let [query (eval qry)
        name (name (first query))
        query-name (first query)
        named-query (symbol (str "list-" entname "-query"))
        fn-name (str "query-" entname "-" name)
        params (get-query-paramnames (second query))]
    `(defn ~(symbol fn-name)
       ~(str "A function to call a named query " fn-name)
       [~@(map symbol params)]
       (~named-query ~(keyword query-name) ~(into {} (map (fn [i] {(keyword i) (symbol i)}) params))))))

(defmacro defnamedqueries
  "Converts an entity's named queries to functions"
  [entname queries]
  (let [fndefs (map (partial get-fndef entname) queries)]
    `(do
       ~@fndefs
       )))


(defmacro defentityrecord
  [nm entdef fields opts]
  (let [entname (name nm)
        fieldnames (map #(symbol (first %)) fields)
        fieldkeys (map #(keyword (first %)) fields)
        tablename (:tablename entdef)
        mtmlinks (:manytomany entdef)
        queries (:queries entdef)]
    ; Install many-to-many link entity first...
    
    `(do
       (many-to-many-definition ~entname ~mtmlinks)
       
       ;(defrecord ~(symbol (str nm "-record")) [~@fieldnames] ~@opts)
       
       (defn ~(symbol (str "new-" entname))
         ~(str "Create a new instance of " entname ", using applicable hashmap values")
         [{:keys [~@fieldnames] :as valuemap#}]
         (new-entity [~@fieldkeys] valuemap#)
         
         ;; Removed record support until required.
         ;(~(symbol (str *ns* "." entname "-record.")) ~@fieldnames)
         )
       
       (defn ~(symbol (str "find-" entname "-by"))
         ~(str "Find a single instance " entname " by a specified field")
         [fld# val#]
         (let [result# (find-by fld# ~tablename val#)]
           (if (empty? result#) nil
             (~(symbol (str  "new-" entname)) result#))))

       (defn ~(symbol (str "delete-" entname "-by"))
         ~(str "Delete a single instance " entname " by a specified field")
         [field# value#]
         (delete-by field# ~tablename value#))
       
       (defn ~(symbol (str "find-or-create-" entname ))
         ~(str "Find a single instance " entname " by a specified field")
         [fld# val#]
         (~(symbol (str  "new-" entname)) (find-or-create  fld# ~tablename val#)))

       (defn ~(symbol (str "save-" entname))
         ~(str "Save " entname ", using applicable hashmap values")
         [entity#]
         (save-entity ~tablename entity#))

       (defn ~(symbol (str "list-" entname))
         ~(str "Simply list all of " entname "")
         []
         (get-list ~tablename))
       
       (defn ~(symbol (str "cached-" entname))
         ~(str "Get " entname " by uuid, caching it if not already cached.")
         [uuid#]
         (get-cached-entity-id ~tablename uuid#))

       (defmacro ~(symbol (str "with-" entname "-result"))
         ~(str "Run a named query for the entity, " entname "")
         [rs# querykey# param-map# ~'& body#]
         `(with-namequery-result ~rs# (keyword ~~entname) ~querykey# ~param-map# ~@body#)
         )
       
       (defn ~(symbol (str "list-" entname "-query"))
         ~(str "Run a named query for the entity, " entname ", realize and return all its results. Note that this is slower and more memory intensive than using with-" entname "-result, so use carefully")
         [querykey# param-map#]
         (with-namequery-result rs# (keyword ~entname) querykey# param-map#
           (doall rs#)))

       ))
  )



(defn process-field [field]
  (let [[nm tp & opts] field
        nm (name nm)]
    (cond
     (= :link tp) [nm tp (name (first opts))]
     :else (apply vector nm tp opts))))

(defn process-index [index]
  (let [[nm tp & opts] index
        nm (name nm)]
    [nm tp (into [] (map #(apply vector (name (first %)) (rest %)) (first opts)))]
    ))

(defn convert-to-def [nm opts]
  (let [mapped (apply merge (map #(hash-map (keyword (name (first %))) (vec (rest %))) opts))]
    (assoc mapped
      :extends (into [] (map #(name %) (:extends mapped)))
      :fields (into [] (map process-field (:fields mapped)))
      :manytomany (into [] (map #(name %) (:manytomany mapped)))
      :post-create-fn (apply vector (:post-create-fn mapped))
      :queries (into [] (:queries mapped))
      :indices (into [] (map process-index (:indices mapped)))
      )))

(defn merge-entitydef-super [base supernm]
  (if-let [super ((keyword supernm) @entitydefs)]
    (do
      (merge-with into super base)
      )
     base
    ))

(defn convert-with-extends [nm opts]
  (let [converted (convert-to-def nm opts)
        extended (:extends converted)]
    (if extended
      (reduce merge-entitydef-super converted extended)
      converted)))

(defn entity-register! [nm entitydef]
  (swap! entitydefs assoc (keyword (name nm)) entitydef))

(defmacro defentity-template [nm & opts]
  (let [entitydef (convert-to-def nm opts)]
    `(entity-register! ~(name nm) ~entitydef)))

(defn colname
  "Transform to valid column name"
  [col]
  (.replaceAll (name col) "-" "_"))


(defmacro defentity [nm & opts]
  (let [entitydef (convert-with-extends nm opts)
        tablename (if (:tablename entitydef) (first (:tablename entitydef)) (colname nm)) 
        entitydef (assoc entitydef
                    :name (name nm)
                    :tablename tablename
                    :queries (apply hash-map (:queries entitydef)))
        fields (:fields entitydef)
        recopts (:record-body entitydef)]
    `(do
       (entity-register! ~(name nm) ~entitydef)
       (defentityrecord ~nm ~entitydef [~@fields] [~@recopts])
       (defnamedqueries ~(name nm) ~(:queries entitydef))
       )))

(def coltypes
  {:version "BIGINT DEFAULT 0 NOT NULL"
   :currency "BIGINT"
   :auto-index "BIGINT PRIMARY KEY AUTO_INCREMENT"
   :uuid "varchar(36)"
   :unique-uuid "varchar(36) UNIQUE"
   :full-varchar "varchar(255)"
   :password "varchar(32)"
   :enum "ENUM"
   :email "varchar(255)"
   :ipaddress "varchar(32)"
   :link "BIGINT"
   :boolean "tinyint(1)"})

(def default-indextypes
  {:uuid :unique
   :text :fulltext
   :mediumtext :fulltext
   :longtext :fulltext})

(defn join-str-with [prefix sep coll suffix]
  (str prefix (apply str (interpose sep coll)) suffix))

(defn get-enum-value [cell]
  (cond
   (= (first cell) 'quote) (eval (eval cell))
   :else cell))

(defn map-coltype [fielddesc]
  (let [coltype (second fielddesc)]
    (cond
     (= coltype :enum) (join-str-with "ENUM('" "', '" (get-enum-value (nth fielddesc 2)) "')")
     (contains? coltypes coltype) (coltypes coltype)
     :else coltype
     )
    )
  )

(defn defmtmlink
  "Create basic many to many linking entity definition."
  [entname link-tablename left-entname right-entname]
  `(defentity ~entname
     (tablename ~(str link-tablename))
     (fields
      [~(symbol (str left-entname "_id")) :link ~(symbol left-entname)]
      [~(symbol (str right-entname "_id")) :link ~(symbol right-entname)])))

(defmacro many-to-many-definition [entname mtmlinks]
  (if (> (count mtmlinks) 0)
    (let [links (for [lname mtmlinks
                      :let [link-entname (str entname "-" lname)
                            link-tablename (str entname "_" lname)
                            link-key (keyword link-tablename)]]
                  (defmtmlink (symbol link-entname) (symbol link-tablename) entname lname))]
      `(do ~@links)
      )
    
    ))

(defn map-colname [fielddesc]
  (keyword (colname (first fielddesc)))
  )

(defn index-colnames [[col link flds]]
  [(keyword (colname col)) link (map #(into [(keyword (colname (first %)))] (rest %)) flds)]
  )

(defn convert-field-to-index
  "Convert a single field to index definition"
  [fld]
  (let [fldname (first fld)
        fldtype (second fld)
        indextypes (some #{:index :unique :fulltext :spatial} fld)
        indextype (cond
                   (not-empty indextypes) (first indextypes)
                   (contains? default-indextypes fldtype) (default-indextypes fldtype)
                   :else :index)]
    [fldname indextype [[fldname]]]))

(defn index-fields
  "Get a list of indexed fields on a list of column definitions and transform it to [idxname idxtype & cols]"
  [flds]
  (map convert-field-to-index (filter #(some #{:indexed :link} %) flds)))

(defn find-indices
  "Get a list of all the indices that should be maintained for this entity"
  [entitydef]
  (let [indices (:indices entitydef)
        flds (:fields entitydef)
        indexed-fields (index-fields flds)]
    (map index-colnames (into indices indexed-fields))))

(defn convert-field-to-fk
  "Convert a single field definition to foreign key"
  [fld]
  (let [opts (get fld 3)
        column (name (first fld))
        foreign-ent (@entitydefs (keyword (nth fld 2)))
        fk-table (:tablename foreign-ent)
        fk-column (name (get opts :column :id))]
    [(colname column) fk-table (colname fk-column) opts]
    )
  )

(defn find-foreignkeys
  "Get a list of all foreign keys that should be maintained for this entity"
  [entitydef]
  (map convert-field-to-fk (filter #(some #{:link} %) (:fields entitydef))))


(defn add-entity-triggers [entitydef]
  (add-default-triggers (:tablename entitydef)))

(defn update-entity-table
  "Get the actions for updating entity table"
  [entitydef]
  (let [enname (:name entitydef)
        tablename (:tablename entitydef)
        exists (table-exists tablename)
        flds (:fields entitydef)
        fielddesc (map #(vector (map-colname %) (map-coltype %)) flds)]

    {;; Update columns
     :entity entitydef
     :table
     #(do
        (debug "Updating" tablename)
        (apply update-table (colname tablename) fielddesc))
     
     ;; Apply indices
     :indices
     #(doseq [index (find-indices entitydef)]
       (debug "Adding index:" index)
       (apply add-index tablename index))

     ;; Apply foreign keys
     :fk
     #(doseq [fk (find-foreignkeys entitydef)]
       (debug "Adding FK:" fk)
       (apply add-foreignkey tablename fk))

     ;; Post create function if applicable
     :post-create
     #(if (not exists)
        (if-let [post-fns (:post-create-fn entitydef)]
          (doseq [post-fn post-fns]
            (debug "Running post-create-fn:" post-fn)
            (eval `(~post-fn (@entitydefs ~(keyword (str enname)))))
            )))
     }))

(defn batch-update-entities- [entitylist]
  (info "Updating entities:" (count entitylist))
  (let [tables (doall (for [def entitylist]
                        (do
                          (debug "Loading table: " (first def))
                          (update-entity-table (second def)))))]
    (doseq [op [:table :indices :fk :post-create]
            t tables]
      (debug "Handling" op ":" (-> t :entity :name))
      ((op t))
      ))
  (info "Completed entities update"))

(defn update-entity-tables!
  "Goes through the entitydefs and update all the tables"
  []
  (batch-update-entities- (filter #(-> (second %) :tablename) @entitydefs))
)

(defn param-value
  "Get the value of a key on param map, or throw a runtime exception if not found. Fail-fast."
  [entkey querykey param-map qry pmap paramkey]
  (if-let [result (param-map paramkey)]
    (if (coll? result)
      (if (not-empty result)
        (apply conj pmap result))
      (conj pmap result))
    (throw (new RuntimeException
                (str paramkey " not passed for query on "
                     entkey ", " querykey " with param map "
                     param-map " on query: " qry))))
  )

(defn substitute-param-keys
  "Replace :param-keys with ?'s - If the param-value in question is a collection, it will create a ? for each entry."
  [query paramkeys param-map]
  (reduce
   (fn [qry key]
     (let [value (get param-map key "")]
       (if (coll? value)
         (.replaceAll qry (str key) (join-str-with "" "," (repeat (count value) "?") ""))
         (.replaceAll qry (str key) "?"))))
   query paramkeys))

(defn get-named-query
  "Get a named query from an entity and return the SQL + parameters ready for use in sql/with-query-results."
  [entkey querykey param-map]
  {:pre [(map? param-map)]}
  (if-let [query (-> (@entitydefs entkey) :queries querykey)]
    (let [paramkeys (get-query-paramkeys query)
          paramquery (substitute-param-keys query paramkeys param-map)
          pmapping (partial param-value entkey querykey param-map query)]
      (into [paramquery] (reduce pmapping [] paramkeys)))
    (throw (RuntimeException. (str "Query " entkey " " querykey " was not found."))))
    )

(defmacro with-namequery-result
  "Wrap the building of the named query into a with-query-results - binds the result seq as rs, identical as with-query-results"
  [rs entname queryname param-map & body]
  `(let [sql-params# (get-named-query ~entname ~queryname ~param-map)]
     (trace sql-params#)
     (sql/with-query-results ~rs sql-params#
       ~@body
       )))

(defn enum-from-code [entkey fldkey code]
  (let [filterfn (partial filter #(= fldkey (keyword (first %))))
        entriesfn #(nth % 2)
        enumfld (-> (@entitydefs entkey) :fields filterfn first entriesfn eval eval)
        enum (get enumfld code :unknown)]
    (case enum
      :unknown (do (warn "Unknown enum value" code "for enum" entkey fldkey "in array" enumfld) 0)
      enum)
     ))

(comment
  ;; Example of sql name querying
  
  (sql/with-connection db
    ;; Expanded format, nothing fancy.
    (let [sql-params (get-named-query :user :group {:group-id 2})]
      (sql/with-query-results rs sql-params
        (doseq [user rs]
          (println "user" (:username user) "is in group")))))

  (sql/with-connection db
    ;; With macro
    (with-namequery-result rs :user :group {:group-id 2}
      (doseq [user rs]
        (println "user" (:username user) "is in group")))))
