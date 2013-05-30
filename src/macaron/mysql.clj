(ns macaron.mysql
  (:use [clojure.tools.logging])
  (:require [macaron.impl.mysql :as impl])
  (:require [clojure.java.jdbc :as sql])
  (:require [clojure.string :as string])
  (:require [clojure.set :as set]))

(defn add-default-triggers [{table :tablename :as entitydef}]
  (sql/do-commands (str "DROP TRIGGER IF EXISTS before_insert_" table))
  (sql/do-commands (str "
    CREATE TRIGGER before_insert_" table " BEFORE INSERT ON " table " FOR EACH ROW 
    BEGIN
     IF new.uuid IS NULL THEN 
       SET new.uuid = uuid();
     END IF;

     IF new.dateadded IS NULL THEN
       SET new.dateadded = now();
     END IF;
    END")))

(defn query
  [docstring? & [qrystring]]
  (let [query (or qrystring docstring?)
        doc (if qrystring docstring? "")
        queryparts (seq (.split query "[:]([A-Za-z0-9+*_-]+)+"))
        paramparts (map (comp keyword second) (re-seq #"[:]([A-Za-z0-9+*_-]+)" query))]
    {:woven (impl/interweave queryparts paramparts)
     :params (map keyword paramparts)
     :query queryparts}))

(defmacro with-connection [db & body]
  `(sql/with-connection ~db ~@body))

(defmacro defwith-db-fn [db]
  `(defmacro ~(symbol "with-db") [& body#]
     `(sql/with-connection ~~db (sql/transaction ~@body#))))

(defn update-entity-tables!
  "Goes through the entitydefs and update all the tables"
  [entitydefs]
  (impl/batch-update-entities (filter #(-> (second %) :tablename) entitydefs)))

(defn save [{table :tablename :as entdef} entity]
  (if (and (contains? entity :id) (:id entity))
    (if (contains? entity :version)
      (impl/update-entity-with-version entdef entity)
      (do (sql/update-values table ["id=?" (:id entity)] entity) entity))
    (assoc entity :id (:generated_key (sql/insert-record table entity)))))

(defn save-list [{table :tablename :as entdef} entities]
  (doall
   (for [entity entities]
     (save table entity))))

(defn find-by
  [{tablename :tablename} idcol value]
  (let [result (sql/with-query-results rs
                 [(format "SELECT * FROM %s WHERE %s = ?" tablename (sql/as-identifier idcol)) value]
                 (into {} rs))]
    (if (empty? result)
      nil result)))

(defn delete-by
  "Delete an entity using the specified column provided"
  [{table :tablename} col value]
  (sql/delete-rows table [(format "%s = ?" (sql/as-identifier col)) value]))

(defn list-all
  "Get a simple list of entities from the database."
  [{table :tablename}]
  (sql/with-query-results rs [(format "SELECT * FROM %s" table)] (apply list rs)))

(defn list-query
  [entdef querykey & [param-map]]
  (let [sql-params (impl/get-named-query entdef querykey param-map)]
     (sql/with-query-results rs sql-params
       (doall rs))))

(defn find-or-create [entdef idcol entity]
  {:pre (map? entity)}
  (if-let [id (get entity idcol)]
    (let [ent (find-by entdef idcol id)]
      (if (nil? ent)
        (save entdef entity)
        ent))
    (throw (RuntimeException. (str "Id column " idcol " not found on entity " entity)))))


