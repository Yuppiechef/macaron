; Some examples of using Macaron to do your SQL business...
(ns macaron.example
  (:require [macaron.core :as mc])
  (:require [clojure.java.jdbc :as sql])
  (:require [clojure.tools.logging :as log])
  (:import (java.util Date)
           (java.sql SQLException)))

(defn check-test-tables
  "Run this function to create/update your tables. Load this namespace and wrap this call as: (with-db (check-test-tables))"
  []
  (mc/update-entity-tables!))

; Which database you want to connect to This would typically be controlled by environment proprties and not be hard coded like here
; Database here is yc_test with user/password: yc
(def db {:classname "com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource"
         :subprotocol "mysql"
         :subname "//localhost:3306/yc_test?useUnicode=true&amp;characterEncoding=UTF8"
         :user "yc"
         :password "yc"
         :min-connections 0
         :max-connections 5
         :inactivity-timeout 200
         :wait-timeout 10})
; TODO fix up with foreman to load an .env file for testing purposes
(comment
  "Need to get Foreman working for tests"
  (def db {:classname "com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource"
                  :subprotocol "mysql"
                  :subname (str "//" (System/getenv "DB_TEST_HOST") ":" (System/getenv "DB_TEST_PORT") "/" (System/getenv "DB_TEST_NAME") "?useUnicode=true&amp;characterEncoding=UTF8")
                  :user (System/getenv "DB_TEST_USERNAME")
                  :password (System/getenv "DB_TEST_PWD")
                  :min-connections 5
                  :max-connections 50
                  :inactivity-timeout 200
                  :wait-timeout 10}))

; Simple macro to execute your functions within a transaction from the database
; Try and let the higher up code in your application call this macro to create the transaction at the correct/upper level and not
; multiple transactions that will not see each other's data, etc
(defmacro with-db
  [& body]
  `(try
     ;(log/info "Executing function: " ~body)
     (sql/with-connection db (sql/transaction ~@body))
     ;(catch SQLException e#
       ;(log/error "Error executing query:" e#)
       ;(throw e#)
     ))

; The base data enity that all your other data entities will extends.
; This defines all the common columns for your tables
; Note that if your tablename is the same as your entity name, youdon't need to specify (tablename). 
; It's just if you have mysql being difficult with the name (like 'user'), you can specify another name ('users')
(mc/defentity-template base-entity
           (fields
            [id :auto-index]
            [version :version]
            [uuid :unique-uuid :indexed]
            [dateadded :datetime])
           (post-create-fn macaron.core/add-entity-triggers))

; A very simple types table
(mc/defentity type1
            (tablename "type1")
            (extends base-entity)
            (fields
             [name :full-varchar])
            (queries
             :all "SELECT * FROM type1"
             :summary "SELECT id, name FROM type1"))

; A simple data table that links to the type1 table
(mc/defentity data1
            (tablename "data1")
            (extends base-entity)
            (fields
             [name :full-varchar] 
             [description :full-varchar]
             [status :enum ["active" "inactive"]]
             [type1_id :link type1])            
            (queries
             :status "SELECT id, name, description FROM data1 where status = :status"))

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
             :important "SELECT important_column FROM data2 where id = :id or name = :name"
             :joined "SELECT dd.data1_id FROM data2 d2 left join data2_data1 dd where dd.data2 = :d1id"))

; Another simple table with multiple enums
(mc/defentity data3
            (tablename "data3")
            (extends base-entity)
            (fields             
             [name :full-varchar]
             [sometext :full-varchar]
             [country :enum ["sa" "uk"] ]
             [gender :enum ["male" "female"]])            
            (queries
             :all-summary "SELECT id, country, gender FROM data3"))

; Macaron provides some useful functions for each of your entities
; that are generated into your current namespace by macros
; Again calls to these functions need to be within a (with-db (...))
(defn get-type1-summaries
  "Loads all the type1 rows but specific columns only using a named query"
  []
  (list-type1-query :summary {}))

(defn get-type1
  "Loads all the type1 rows"
  []
  (list-type1-query :all {}))

(defn save-type
  "Saves a new type, eg: (with-db(save-type name))"
  [name]
  (save-type1 {:dateadded (Date.) :name name}))

(defn update-type
  "Updates an existing type"
  [id name]
  (save-type1 {:id 1 :name name}))

(defn save-1st-data
  "Save a data1 row Note: the column name of the type1_id and not type1-id which is the name in the entity"
  [name description status type-id]
  (save-data1 {:dateadded (Date.) :name name :description description :status status :type1_id type-id}))

(defn save-2nd-data
  "Save a data2 row"
  [name description important]
  (save-data2 {:dateadded (Date.) :name name :description description :important_column important}))

(defn link-data1-to-data2
  "Link up a data1 and a data2 instance"
  [data1-id data2-id]
  (save-data2-data1 {:data1_id data1-id :data2_id data2-id}))

(defn generate-sqlexception
  "Incorrect column name generates a SQLException for testing purposes"
  [data1-id data2-id]
  (save-data2-data1 {:data1_idNot data1-id :data2_id data2-id}))

(defn all-named-queries
  ""
  []
  (find-data1-by)
  (mc/deftest1)
  (test1 "Bob")
                                        ;(query-type1-all)
  )










