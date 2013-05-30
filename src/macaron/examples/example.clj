(ns macaron.exmples.example
  (:use [macaron.core])
  (:require [macaron.mysql :as mysql]))

(def db {:classname "com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource"
         :subprotocol "mysql"
         :subname (str "//localhost:3306/test?useUnicode=true&amp;characterEncoding=UTF8")
         :user "root"
         :password "root"
         :min-connections 5
         :max-connections 50
         :inactivity-timeout 200
         :wait-timeout 10})

(mysql/defwith-db-fn db)

(defentity-template base-entity
  (fields
   [id :auto-index]
   [version :version]
   [uuid :unique-uuid :indexed]
   [dateadded :datetime])
  {:post-create-fn [#'mysql/add-default-triggers]})

(defentity group
  (tablename "groups")
  (extends base-entity)
  (fields
   [refid :bigint]
   [name :full-varchar]
   [permissions :text])
  (queries
   mysql/query
   :links
   ["SELECT user_id FROM user_group WHERE group_id=:group-id"]))

(defentity user
  (tablename "users")
  (extends base-entity)
  (fields
   [refid :bigint]
   [username :full-varchar]
   [firstname :full-varchar]
   [lastname :full-varchar]
   [pwd :password]
   [email :email]
   [authcode :full-varchar])
  (indices
   [firstname_lastname :index [[firstname] [lastname]]])
  (manytomany group)
  (queries
   mysql/query
   :refid ["SELECT * FROM users WHERE refid IN (:user-ids)"]
   :group ["SELECT u.* FROM user_group ul, users u WHERE ul.group_id=:group-id AND u.id=ul.user_id"]
   :links ["SELECT * FROM user_group WHERE user_id=:user-id AND group_id in (:group-id) AND user_id in (:user-id)"]
   :permissions ["SELECT g.permissions perms FROM groups g, user_group gl WHERE gl.user_id=:user-id AND gl.group_id=g.id"]
   :authcodelist ["SELECT u.firstname, u.lastname, u.authcode, u.email FROM users u, user_group ug, groups g WHERE g.name='Warehouse Level 1' and g.id=ug.group_id and u.id=ug.user_id"]))

(defentity document
  (extends base-entity)
  (fields
   [title :full-varchar]
   [text :text]
   [user_id :link user]
   [status :enum ["Open" "Closed" "Pending"]])
  {:doc "To show that you can attach arbitrary parts to an entity"})
