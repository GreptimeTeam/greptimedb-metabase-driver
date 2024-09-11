(ns metabase.driver.greptimedb
  (:refer-clojure :exclude [second])
  (:require
   [clojure.java.jdbc :as jdbc]
   [clojure.set :as set]
   [clojure.string :as str]
   [honey.sql :as sql]
   [honey.sql.helpers :as sql.helpers]
   [java-time.api :as t]
   [medley.core :as m]
   [metabase.driver :as driver]
   [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.util :as u]
   [metabase.util.date-2 :as u.date]
   [metabase.util.honey-sql-2 :as h2x]
   [metabase.util.log :as log]))

(set! *warn-on-reflection* true)

(driver/register! :greptimedb, :parent #{:sql-jdbc})

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                          metabase.driver method impls                                          |
;;; +----------------------------------------------------------------------------------------------------------------+

(doseq [[feature supported?] {:datetime-diff                 true
                              :nested-fields                 false
                              :uuid-type                     false
                              :connection/multiple-databases true
                              :identifiers-with-spaces       false
                              :metadata/key-constraints      false
                              :test/jvm-timezone-setting     false}]
  (defmethod driver/database-supports? [:greptimedb feature] [_driver _feature _db] supported?))

(defmethod driver/display-name :greptimedb [_] "GreptimeDB")

(defmethod driver/db-default-timezone :greptimedb
  [driver database]
  (sql-jdbc.execute/do-with-connection-with-options
   driver database nil
   (fn [^java.sql.Connection conn]
     (with-open [stmt (.prepareStatement conn "SELECT timezone();")
                 rset (.executeQuery stmt)]
       (when (.next rset)
         (.getString rset 1))))))

(defmethod driver/db-start-of-week :greptimedb
  [_]
  :monday)

(defn- get-tables-sql
  [driver schemas table-names]
  (sql/format
   (cond-> {:select [[:table_name :name]
                     [:table_schema :schema]
                     [:table_type :type]
                     [:table_comment :description]]
            :from [:information_schema.tables]
            :where [:not-in :table_schema (sql-jdbc.sync/excluded-schemas driver)]}

     (seq schemas)
     (sql.helpers/where [:in :table_schema schemas])

     (seq table-names)
     (sql.helpers/where [:in :table_name table-names]))))

(defmethod driver/describe-database :greptimedb
  [driver database]
  (let [tables (sql-jdbc.execute/reducible-query
                database (get-tables-sql driver nil nil))]
    {:tables (into #{} tables)}))

(defn- get-columns-sql
  [_driver schema table-name]
  (sql/format
   {:select [:table_schema
             :table_name
             [:column_name :name]
             [:greptime_data_type :database_type]
             [:column_comment :field_comment]
             :is_nullable]
    :from [:information_schema.columns]

    :where [:and [:= :table_name table-name]
            [:= :table_schema schema]]}))

(defmethod driver/describe-table :greptimedb
  [driver database table]
  (let [{schema :schema table-name :name} table
        columns (into [] (sql-jdbc.execute/reducible-query
                          database (get-columns-sql driver schema table-name)))
        fields (into [] (map-indexed
                         (fn [idx col]
                           (let [column-name (:name col)
                                 column-db-type (keyword (u/lower-case-en (:database_type col)))]
                             {:table-name (:table_name col)
                              :table-schema (:table_schema col)
                              :name (:name col)
                              :database-type (:database_type col)
                              :field-comment (:field_comment col)
                              :data-required? (= "Yes" (:is_nullable col))
                              :database-position idx
                              :base-type (sql-jdbc.sync/database-type->base-type driver column-db-type)
                              :semantic-type (sql-jdbc.sync/column->semantic-type driver column-db-type column-name)}))
                         columns))]
    {:name table-name
     :schema schema
     :fields fields}))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                     metabase.driver.sql-jdbc method impls                                      |
;;; +----------------------------------------------------------------------------------------------------------------+

;;; ---------------------------------------------- sql-jdbc.connection -----------------------------------------------

(defmethod sql-jdbc.conn/connection-details->spec :greptimedb
  [_driver {:keys [host port user password dbname], :as details}]
  (-> (merge
       {:classname      "org.postgresql.Driver"
        :subprotocol    "postgresql"
        :subname        (str "//" host ":" port "/" dbname)
        :user           user
        :password       password
        :sslmode        "prefer"
        :assumeMinServerVersion  "16"
        :preferQueryMode  "simple"
        :readOnly         true}
       (dissoc details :dbname))
      (sql-jdbc.common/handle-additional-options details)))

(defmethod sql-jdbc.sync/excluded-schemas :greptimedb [_driver]
  #{"greptime_private" "information_schema" "pg_catalog"})

;;; ------------------------------------------------- sql-jdbc.sync --------------------------------------------------

;; Map of column types -> Field base types
(defmethod sql-jdbc.sync/database-type->base-type :greptimedb
  [_driver database-type]
  ({:int8                                :type/Integer
    :int16                               :type/Integer
    :int32                               :type/Integer
    :int64                               :type/BigInteger
    :uint8                               :type/Integer
    :uint16                              :type/Integer
    :uint32                              :type/Integer
    :uint64                              :type/BigInteger
    :decimal                             :type/Decimal

    :float32                             :type/Float
    :float64                             :type/Float

    :string                              :type/Text
    :binary                              :type/*

    :boolean                             :type/Boolean

    :date                                :type/Date
    :datetime                            :type/DateTime
    :timestampsecond                     :type/DateTime
    :timestampmillisecond                :type/DateTime
    :timestampmicrosecond                :type/DateTime
    :timestampnanosecond                 :type/DateTime}
   database-type))

(defmethod sql-jdbc.sync/column->semantic-type :greptimedb
  [_ database-type _]
  ;; More types to be added when we start caring about them
  ({:int8                                :type/Quantity
    :int16                               :type/Quantity
    :int32                               :type/Quantity
    :int64                               :type/Quantity
    :uint8                               :type/Quantity
    :uint16                              :type/Quantity
    :uint32                              :type/Quantity
    :uint64                              :type/Quantity
    :decimal                             :type/Quantity

    :float32                             :type/Quantity
    :float64                             :type/Quantity

    :string                              :type/Category
    :binary                              :type/*

    :boolean                             :type/*

    :date                                :type/CreationDate
    :datetime                            :type/CreationTimestamp
    :timestampsecond                     :type/CreationTimestamp
    :timestampmillisecond                :type/CreationTimestamp
    :timestampmicrosecond                :type/CreationTimestamp
    :timestampnanosecond                 :type/CreationTimestamp}
   database-type))

;;; ------------------------------------------------ sql-jdbc execute ------------------------------------------------

(defmethod sql-jdbc.execute/set-timezone-sql :greptimedb
  [_]
  "SET time_zone = %s;")

;;; ------------------------------------------------- date functions -------------------------------------------------

;;;; Datetime truncation functions

;;; If `expr` is a date, we need to cast it to a timestamp before we can truncate to a finer granularity Ideally, we
;;; should make this conditional. There's a generic approach above, but different use cases should b tested.
(defmethod sql.qp/date [:greptimedb :minute]  [_driver _unit expr] [:date_trunc (h2x/literal :minute) expr])
(defmethod sql.qp/date [:greptimedb :hour]    [_driver _unit expr] [:date_trunc (h2x/literal :hour) expr])
(defmethod sql.qp/date [:greptimedb :day]     [_driver _unit expr] [:date_trunc (h2x/literal :day) expr])
(defmethod sql.qp/date [:greptimedb :month]   [_driver _unit expr] [:date_trunc (h2x/literal :month) expr])
(defmethod sql.qp/date [:greptimedb :quarter] [_driver _unit expr] [:date_trunc (h2x/literal :quarter) expr])
(defmethod sql.qp/date [:greptimedb :year]    [_driver _unit expr] [:date_trunc (h2x/literal :year) expr])

(defmethod sql.qp/date [:greptimedb :week]
  [driver _ expr]
  (sql.qp/adjust-start-of-week driver (partial conj [:date_trunc] (h2x/literal :week)) expr))

;;;; Datetime extraction functions

(defmethod sql.qp/date [:greptimedb :minute-of-hour]  [_driver _unit expr]
  [::h2x/extract :minute expr])
(defmethod sql.qp/date [:greptimedb :hour-of-day]     [_driver _unit expr]
  [::h2x/extract :hour expr])
(defmethod sql.qp/date [:greptimedb :day-of-month]    [_driver _unit expr]
  [::h2x/extract :day expr])
(defmethod sql.qp/date [:greptimedb :day-of-year]     [_driver _unit expr]
  [::h2x/extract :doy expr])
(defmethod sql.qp/date [:greptimedb :month-of-year]   [_driver _unit expr]
  [::h2x/extract :month expr])
(defmethod sql.qp/date [:greptimedb :quarter-of-year] [_driver _unit expr]
  [::h2x/extract :quarter expr])
(defmethod sql.qp/date [:greptimedb :day-of-week]
  [driver _ expr]
  (sql.qp/adjust-day-of-week driver [::h2x/extract :dow expr]))

(defmethod sql.qp/unix-timestamp->honeysql [:greptimedb :seconds]
  [_driver _seconds-or-milliseconds expr]
  [:from_unixtime expr])

;; (defmethod sql.qp/cast-temporal-string [:greptimedb :Coercion/ISO8601->DateTime]
;;   [_driver _semantic-type expr]
;;   (h2x/->timestamp expr))

;; (defmethod sql.qp/cast-temporal-string [:greptimedb :Coercion/ISO8601->Date]
;;   [_driver _semantic-type expr]
;;   (h2x/->date expr))

;; (defmethod sql.qp/cast-temporal-string [:greptimedb :Coercion/ISO8601->Time]
;;   [_driver _semantic-type expr]
;;   (h2x/->time expr))

(defmethod sql.qp/->honeysql [:greptimedb :percentile]
  [driver [_ arg p]]
  [:approx_percentile_cont (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver p)])

(defmethod sql.qp/->honeysql [:greptimedb :regex-match-first]
  [driver [_ arg pattern]]
  [:regexp_match (sql.qp/->honeysql driver arg) pattern])

(defn- interval [amount unit]
  [:interval (format "%s %s" amount (name unit))])

(defmethod sql.qp/add-interval-honeysql-form :greptimedb
  [driver hsql-form amount unit]
  (if (= unit :quarter)
    (recur driver hsql-form (* 3 amount) :month)
    (-> (h2x/+ hsql-form (interval amount unit))
        (h2x/with-type-info (h2x/type-info hsql-form)))))
