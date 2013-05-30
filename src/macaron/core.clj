(ns macaron.core
  (:use [clojure.tools.logging])
  (:require [macaron.impl.core :as impl])
  (:require [clojure.string :as string])
  (:require [clojure.set :as set]))

(def coltypes
  #{:version :currency :auto-index :uuid :unique-uuid :full-varchar
    :password :enum :email :ipaddress :link :boolean :date})

(defn tablename [nm]
  {:tablename nm})

(defn extends [& entities]
  {:extends entities})

(defn queries [qfn & name-query-pairs]
  (let [pairs (partition 2 name-query-pairs)
        keys (map first pairs)
        entries (map (comp #(if (coll? %) (apply qfn %) (qfn %)) second) pairs)]
    {:queries (zipmap keys entries)}))

(defmacro fields [& fielddefs]
  (let [defs (into {} (map impl/process-field fielddefs))]
    `{:fields ~defs}))

(defmacro indices [& indices]
  (let [defs (into {} (map impl/process-index indices))]
    {:indices
     defs}))

(defmacro manytomany [& links]
  `{:manytomany ~(vec links)})


(defn defmtmlink-
  "Create basic many to many linking entity definition."
  [entname link-tablename left-entname right-entname]
  `(defentity ~(symbol entname)
     (tablename ~(str link-tablename))
     (fields
      [~(symbol (str left-entname "_id")) :link ~(symbol left-entname)]
      [~(symbol (str right-entname "_id")) :link ~(symbol right-entname)])))

(defn many-to-many-definition- [{entname :name mtmlinks :manytomany :as ent}]
  (when (> (count mtmlinks) 0)
    (let [links (for [{lname :name} mtmlinks
                      :let [link-entname (str (name entname) "-" (name lname))
                            link-tablename (str (name entname) "_" (name lname))
                            link-key (keyword link-tablename)]]
                  (defmtmlink- link-entname link-tablename (name entname) (name lname)))]
      (eval `(do ~@links)))))

(defmacro defentity-template [nm & body]
  `(do
     (def ~nm (dissoc (merge ~{:name (name nm)} ~@body) :tablename))
     (impl/entity-register! ~nm)
     ~nm))

(defmacro defentity [nm & body]
  `(do
     (def ~nm (impl/extend-entdef (merge ~{:name (name nm) :tablename (name nm)} ~@body)))
     (impl/entity-register! ~nm)
     (many-to-many-definition- ~nm)
     ~nm))

(defn new-entity
  "Create a new entity hashmap, stripping out all keys not present"
  [{fields :fields} & [valuemap]]
  (select-keys (or valuemap {}) (map first fields)))

(defn enum-from-code [entdef fldkey code]
  (let [enum (get-in entdef [:fields fldkey 2] :unknown)]
    (case enum
      :unknown (do (warn "Unknown enum value" code "for enum" (:name entdef) fldkey "in array" (get-in entdef [:fields fldkey])) 0)
      enum)))
