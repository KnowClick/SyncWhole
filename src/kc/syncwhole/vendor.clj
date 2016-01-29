(ns kc.syncwhole.vendor
  "Defines multimethods for handling differences in DB vendors"
  (:require [clojure.string :as string]
            [clojure.java.jdbc :as jdbc]
            [clj-time.core :as clj-time]
            [honeysql.core :as h]
            [honeysql.helpers :as hh]))

(defmulti quoting-fn
  "Given a db-conf return a function for quoting sql entities,
  as required by jdbc."
  {:arglists '([db-conf])}
  :adapter)

(defmulti quoting-style
  "Given a db-conf return a keyword describing the quoting style used
  by the relevant db vendor, as required by honeysql."
  {:arglists '([db-conf])}
  :adapter)

(defmulti connection-init-sql
  "Given a db-conf return the SQL query to execute at connection initiation time.
  Currently used to make the sure connection's time zone is UTC."
  {:arglists '([db-conf])}
  :adapter)

(defmulti timestamp-precision
  "Return the timestamp precision of the column, probably by querying the
  information_schema.columns table."
  {:arglists '([db-conf table column])}
  (fn [db-conf table column] (:adapter db-conf)))

(defmulti clj-timestamp-truncator
  "Returns a function to truncate a timestamp / datetime object to
  the given precision.
  The precision is an integer that maps to a value like :seconds or :microseconds.
  e.g. {0 :second 6 :microsecond}"
  {:arglists '([precision])}
  identity)

(defmulti sql-timestamp-truncator
  "Returns a function to return the honeySQL expression to truncate a timestamp to
  the given precision.
  The precision is an integer that maps to a value like :seconds or :microseconds."
  {:arglists '([db-vendor precision])}
  (fn [db-vendor precision] [db-vendor precision]))

(defmulti jdbc-uri
  "Returns a jdbc connection uri to use in a db-spec;
  Example:
  (let [db-spec {:connection-uri (jdbc-uri db-conf)}])"
  {:arglists '([db-conf])}
  (fn [db-conf] (or (:adapter db-conf) (:vendor db-conf))))

;;; ---- Built-in implementations of those multimethods ------------------------

(defmethod quoting-fn :default [_] (jdbc/quoted \"))

(defmethod quoting-fn "mysql" [_] (jdbc/quoted \`))


(defmethod quoting-style :default [_] :ansi)

(defmethod quoting-style "mysql" [_] :mysql)


(defmethod connection-init-sql "postgresql" [_] "SET TIME ZONE 0;")

(defmethod connection-init-sql "mysql" [_] "SET time_zone = \"+00:00\";")


(defmethod timestamp-precision "postgresql" [db-conf table column]
  (let [db-spec {:connection-uri (jdbc-uri db-conf)}
        q (-> (hh/select :datetime_precision)
              (hh/from :information_schema.columns)
              (hh/where
               [:= :table_catalog (-> db-conf :database-name name)]
               [:= :table_schema (-> table :schema name)]
               [:= :table_name (-> table :name name)]
               [:= :column_name (name column)])
              (hh/limit 1)
              (h/format :quoting (quoting-style db-conf)))]
    (jdbc/query db-spec q
                :result-set-fn first
                :row-fn :datetime_precision)))

(defmethod timestamp-precision "mysql" [db-conf table column]
  (let [db-spec {:connection-uri (jdbc-uri db-conf)}
        q (-> (hh/select :datetime_precision)
              (hh/from :information_schema.columns)
              (hh/where
               [:= :table_schema (-> db-conf :database-name name)]
               [:= :table_name (-> table :name name)]
               [:= :column_name (name column)])
              (hh/limit 1)
              (h/format :quoting (quoting-style db-conf)))]
    (jdbc/query db-spec q
                :result-set-fn first
                :row-fn :datetime_precision)))


(defmethod clj-timestamp-truncator 0 [_]
  (fn [ts]
    (clj-time/date-time
     (clj-time/year ts)
     (clj-time/month ts)
     (clj-time/day ts)
     (clj-time/hour ts)
     (clj-time/minute ts)
     (clj-time/second ts))))


(defmethod sql-timestamp-truncator ["postgresql" 0] [_ _]
  (fn [sql]
    (h/call :date_trunc "second" sql)))


(defmethod jdbc-uri "postgresql" [db-conf]
  (str "jdbc:postgresql://" (or (:server-name db-conf) "localhost")
       ":" (or (:port db-conf 5432))
       "/" (:database-name db-conf)
       "?user=" (:username db-conf) "&password=" (:password db-conf)))

(defmethod jdbc-uri "mysql" [db-conf]
  (let [hosts nil
        host (:server-name db-conf)
        port (or (:port db-conf) 3306)
        database (:database-name db-conf)
        properties (-> (or (:properties db-conf) {})
                       (assoc "user" (:username db-conf))
                       (assoc "password" (:password db-conf)))]
    (str "jdbc:mysql://"
         ;; deal with [host1][:port1]...[hostn][:portn]
         (string/join "," (map (fn [h] (str (first (keys h)) ":" (first (vals h))))
                               (or hosts [{host port}])))
         "/"
         database
         "?"
         (string/join "&" (map (fn [[k v]] (str k "=" v)) properties)))))

(defn quick-db-spec [db-conf]
  {:connection-uri (jdbc-uri db-conf)})
