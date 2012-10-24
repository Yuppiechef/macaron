(ns macaron.example
  (:require [macaron.core :as mc])
  (:require [clojure.tools.logging :as log]))

(defn create-test-tables
  []
  (mc/update-entity-tables!))

(def db {:classname "com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource"
         :subprotocol "mysql"
         :subname (str "//" (System/getenv "DB_TEST_HOST") ":" (System/getenv "DB_TEST_PORT") "/" (System/getenv "DB_TEST_NAME") "?useUnicode=true&amp;characterEncoding=UTF8")
         :user (System/getenv "DB_TEST_USERNAME")
         :password (System/getenv "DB_TEST_PWD")
         :min-connections 5
         :max-connections 50
         :inactivity-timeout 200
         :wait-timeout 10})

(defmacro with-db
  [& body]
  `(sql/with-connection db (sql/transaction ~@body)))

; The base enity that all the other entities will inherit from or extend
(mc/defentity-template base-entity
           (fields
            [id :auto-index]
            [version :version]
            [uuid :unique-uuid :indexed]
            [dateadded :datetime])
           (post-create-fn macaron.core/add-entity-triggers))

; A simple types table
(mc/defentity type1
            (tablename "type1")
            (extends base-entity)
            (fields
             [name :full-varchar])
            (queries
             :all "SELECT * FROM type1"))

; A data table that links to the type1 table
(mc/defentity data1
            (tablename "data1")
            (extends base-entity)
            (fields
             [name :full-varchar] 
             [description :full-varchar]
             [status :enum ["active" "inactive"]]
             [type1_id :link type1])            
            (manytomany templateparameter)
            (queries
             :active "SELECT id, name, description FROM data1 where status = 'active'"))

; Another data table that has a many-to-many link to the data1 table
(mc/defentity data2
            (tablename "data2")
            (extends base-entity)
            (fields
             [name :full-varchar] 
             [description :full-varchar]
             [important_column :full-varchar])
            (manytomany data1)
            (queries
             :all-summary "SELECT id, name FROM data2"
             :important "SELECT important_column FROM data2 where id = :id"
             :joined "SELECT dd.data1_id FROM data2 d2 left join data2_data1 dd where dd.data2 = :d1id"))

