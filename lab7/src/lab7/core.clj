(ns lab7.core
  (:gen-class)
  (:use [clojure.string]
        [clojure.data.csv]
        [clojure.data.json]))

(require '[clojure.data.csv :as csv]
         '[clojure.java.io :as io]
         '[clojure.data.json :as js])

; ========================================
; The list of file names:

; MANDATORY
; "../data files/mp-posts_full.csv"
; "../data files/map_zal-skl9.csv"
; "../data files/plenary_register_mps-skl9.tsv"

; ADDITIONAL
; "../data files/plenary_vote_results-skl9.tsv"

; EXTRA
; "../data files/mps-declarations_rada.json"

; ========================================
; Just reading functions for files

;function for .csv parsing
(defn readCSV [path]
  (vec (vec (with-open [reader (io/reader path)]
              (doall
                (csv/read-csv reader))))))

;function for .tsv parsing
(defn readTSV [path]
  (apply conj (vector (clojure.string/split
                        (clojure.string/replace
                          (first
                            (vec
                              (doall
                                (line-seq
                                  (io/reader path))))) #"\t" "|") #"\|"))
         (for [string (rest (vec
                              (doall
                                (line-seq
                                  (io/reader path)))))]
           (conj
             (vec
               (butlast (clojure.string/split
                          (clojure.string/replace string #"\t" "/") #"/")))
             (clojure.string/split
               (last
                 (clojure.string/split
                   (clojure.string/replace string #"\t" "/") #"/")) #"\|")))))

;function for .json parsing
(defn readJSON [path]
  (vec (for [line (with-open [reader (io/reader path)]
                    (doall
                      (js/read reader)))]
         (zipmap (for [word (keys line)]
                   (keyword word))
                 (vals line)))))

; Using zipmap to create vector of maps with header line as keys
; for data in the following lines
(defn mapData
  [head & lines]
  (vec (map #(zipmap (map keyword head) %1) lines)))

;general function for parsing .csv, .tsv, .json files
(defn loadFile [path]
  (cond
    (clojure.string/ends-with? path ".csv")  (apply mapData (readCSV path))
    (clojure.string/ends-with? path ".tsv")  (apply mapData (readTSV path))
    (clojure.string/ends-with? path ".json") (readJSON path)
    :else "Incorrect file path or type!"))

; ========================================
; The parsed files and their choice:

; MANDATORY

; columns:
;   :mp_id
;   :full_name
;   :post_name
;   :unit_name
;   :post_title
(def mp-posts_full
  (vec (for [elem (loadFile "../data files/mp-posts_full.csv")]
         (merge elem {:mp_id (read-string (get elem :mp_id))}))))
; columns:
;   :row
;   :col
;   :pos_x
;   :pos_y
;   :title
;   :id_mp
;   :id_fr
(def map_zal-skl9
  (vec (for [elem (loadFile "../data files/map_zal-skl9.csv")]
         (merge elem {:row (read-string (get elem :row))
                      :col (read-string (get elem :col))
                      :pos_x (read-string (get elem :pos_x))
                      :pos_y (read-string (get elem :pos_y))
                      :id_mp (read-string (case (get elem :id_mp)
                                            "" "nil"
                                            (get elem :id_mp)))
                      :id_fr (read-string (case (get elem :id_fr)
                                            "" "nil"
                                            (get elem :id_fr)))
                      }))))

; columns
;   :date_agenda
;   :id_question
;   :id_event
;   :presence
;   :absent
;   :results
(def plenary_register_mps-skl9
  (vec (for [elem (loadFile "../data files/plenary_register_mps-skl9.tsv")]
         (merge elem {
                      :id_question (read-string (get elem :id_question))
                      :id_event (read-string (get elem :id_event))
                      :presence (read-string (get elem :presence))
                      :absent (read-string (get elem :absent))
                      }))))

; ADDITIONAL

; columns
;   :date_agenda
;   :id_question
;   :id_event
;   :for
;   :against
;   :abstain
;   :not_voting
;   :total
;   :presence
;   :absent
;   :results
(def plenary_vote_results-skl9
  (vec (for [elem (loadFile "../data files/plenary_vote_results-skl9.tsv")]
         (merge elem {
                      :presence (read-string (get elem :presence))
                      :against (read-string (get elem :against))
                      :id_question (read-string (get elem :id_question))
                      :for (read-string (get elem :for))
                      :total (read-string (get elem :total))
                      :absent (read-string (get elem :absent))
                      :abstain (read-string (get elem :abstain))
                      :id_event (read-string (get elem :id_event))
                      :not_voting (read-string (get elem :not_voting))
                      }))))

; EXTRA

; columns
;   :mp_id
;   :fullname
;   :year
;   :guid
;   :decl_url
(def mps-declarations_rada
  (vec (for [elem (loadFile "../data files/mps-declarations_rada.json")]
         (dissoc elem :format))))

; Returns the file you want to see
(defn choose_file
  [name]
  (case name
    "mp-posts_full" mp-posts_full
    "map_zal-skl9" map_zal-skl9
    "plenary_register_mps-skl9" plenary_register_mps-skl9
    "plenary_vote_results-skl9" plenary_vote_results-skl9
    "mps-declarations_rada" mps-declarations_rada
    ))


; ========================================
; Implementation for functions in SELECT query

(defn Min
  [file column]
  (let [file_column (for [line file]
                      (get line (keyword column)))
        limit (count file_column)
        is_string (= (type (first file_column)) java.lang.String)]
    (loop [index 0
           min (first file_column)]
      (if (< index limit)
        (recur (+' 1 index)
               (if is_string
                 (if (< (count (nth file_column index)) (count min))
                   (nth file_column index)
                   min)
                 (if (< (nth file_column index) min)
                   (nth file_column index)
                   min)))
        min))))

(defn Sum
  [file column]
  (let [file_column (for [line file]
                      (get line (keyword column)))
        limit (count file_column)]
    (if (not= java.lang.Long (type (get (first file_column) (keyword column))))
      (loop [index 0
             sum 0]
        (if (< index limit)
          (recur (+' 1 index)
                 (+' (nth file_column index) sum))
          sum))
      (throw (Exception. "The values in the 'SUM' function are not digits!")))))

(defn Count
  [file column]
  (let [file_column (for [line file]
                      (get line (keyword column)))]
    (count (remove nil? file_column)))
  )

(defn callFunction
  [column_raw usedJoin usedGroup data]
  ;(println "=====CALLFUNCTION=====")
  ;(print "column_raw: ")
  ;(println column_raw)
  ;(print "usedJoin: ")
  ;(println usedJoin)
  ;(print "usedGroup: ")
  ;(println usedGroup)
  ;(print "data: ")
  ;(println data)
  (let [file_name (get column_raw :file)
        file (if (or usedJoin
                     usedGroup)
               data
               (choose_file file_name))
        column (if usedJoin
                 (str (get column_raw :file)
                      "."
                      (get column_raw :column))
                 (get column_raw :column))]
    (case (get column_raw :function)
      "count" (Count file column)
      "sum" (Sum file column)
      "min" (Min file column)
      )))

(defn callFunctions
  [columns usedJoin usedGroup groupClause data]
  ;(println "=====CALLFUNCTIONS=====")
  ;(print "columns: ")
  ;(println columns)
  ;(print "usedJoin: ")
  ;(println usedJoin)
  ;(print "usedGroup: ")
  ;(println usedGroup)
  ;(print "groupClause: ")
  ;(println groupClause)
  ;(print "data: ")
  ;(println data)
  (if usedGroup
    (let [groupColumn (if usedJoin
                        (str (get groupClause :file)
                             "."
                             (get groupClause :column))
                        (get groupClause :column))
          groupedData (group-by (keyword groupColumn) data)
          groupedDataKeys (keys groupedData)
          functionResultsRaw (for [column columns]
                               (apply merge (for [elem groupedDataKeys]
                                              (assoc {} elem (assoc {} (keyword
                                                                         (str (get column :function)
                                                                              "<"
                                                                              (if usedJoin
                                                                                (str (get column :file)
                                                                                     "."
                                                                                     (get column :column))
                                                                                (get column :column))
                                                                              ">"))
                                                                       (callFunction column usedJoin usedGroup (get groupedData elem)))))))
          functionResults (if (empty? functionResultsRaw)
                            (throw (Exception. "Invalid syntax for GROUP BY: There are no agregation functions in the SELECT query!"))
                            (apply merge (for [groupedDataKey groupedDataKeys]
                                           (assoc {} groupedDataKey (apply merge (for [singleFunctionData functionResultsRaw]
                                                                                   (get singleFunctionData groupedDataKey)))))))]
      ;(print "functionResultsRaw: ")
      ;(println functionResultsRaw)
      ;(print "functionResults: ")
      ;(println functionResults)
      ;(print "groupColumn: ")
      ;(print "groupedData: ")
      ;(print "groupedDataKeys: ")
      ;(println "==============")
      functionResults)
    (apply merge (for [column columns]
                   (assoc {} (keyword (str (get column :function)
                                           "<"
                                           (if usedJoin
                                             (str (get column :file)
                                                  "."
                                                  (get column :column))
                                             (get column :column))
                                           ">"))
                             (callFunction column usedJoin usedGroup data))))))

; ========================================
; Implementation for JOIN queries

; changes the names of columns in the table_name from column_name to table_name.column_name
(defn modifyColumnNames
  [table table_name]
  (vec (for [element table]
         (apply merge (for [col_name (keys element)]
                        (assoc {} (keyword (str table_name
                                                "."
                                                (name col_name)))
                                  (get element col_name)))))))

(def test_column1
  {:field "id"
   :file "test1"})

(def test_column2
  {:field "mp_id"
   :file "test2"})

(def test1
  [
   {:id 1 :1 1}
   {:id 2 :1 2}
   {:id 3 :1 3}
   {:id 4 :1 4}
   ])

(def test2
  [
   {:mp_id 1 :1 1}
   {:mp_id 2 :1 2}
   {:mp_id 2 :1 3}
   {:mp_id 100 :1 4}
   {:mp_id 150 :1 5}
   ])

(defn left-join
  [column1 column2 table1 table2]
  (remove nil? (flatten (let [table (modifyColumnNames table1 (get column1 :file))
                              other_table (modifyColumnNames table2 (get column2 :file))
                              table_count (count table)
                              field1 (keyword (str (get column1 :file) "." (get column1 :field)))
                              field2 (keyword (str (get column2 :file) "." (get column2 :field)))]
                          (for [i (range 0 table_count)]
                            (let [elem1 (nth table i)
                                  elem2_raw (remove nil? (loop [index 0
                                                                limit (count other_table)
                                                                elements []]
                                                           (if (< index limit)
                                                             (recur
                                                               (+' 1 index)
                                                               limit
                                                               (if (= (get elem1 field1)
                                                                      (get (nth other_table index) field2))
                                                                 (conj elements (nth other_table index))
                                                                 elements))
                                                             elements)))
                                  elem2 (if (some some? elem2_raw)
                                          elem2_raw
                                          (vector (apply merge (flatten (for [word (keys (first other_table))]
                                                                          (assoc {} word nil))))))]
                              (if (not= 0 (count elem2))
                                (for [el elem2]
                                  (merge (nth table i) el))
                                nil)))))))

(defn full-outer-join
  [column1 column2 table1_raw table2_raw]
  (let [left_outer_join (vec (left-join column1 column2 table1_raw table2_raw))
        table1 (modifyColumnNames table1_raw (get column1 :file))
        table2 (modifyColumnNames table2_raw (get column2 :file))
        field1 (keyword (str (get column1 :file) "." (get column1 :field)))
        field2 (keyword (str (get column2 :file) "." (get column2 :field)))
        table1_empty_cell (apply merge (for [word (keys (first table1))]
                                         (assoc {} word nil)))
        right_anti_join (vec (remove nil? (for [elem2 table2]
                                            (let [elem2_value (get elem2 field2)
                                                  elem1 (if (some some?
                                                                  (for [el table1]
                                                                    (if (= (get el field1)
                                                                           elem2_value)
                                                                      1
                                                                      nil)))
                                                          nil
                                                          table1_empty_cell)]
                                              (if (some? elem1)
                                                (merge elem1 elem2)
                                                nil)))))]
    (apply conj left_outer_join right_anti_join)))

(defn inner-join
  [column1 column2 table1 table2]
  (remove nil? (flatten (let [table (modifyColumnNames table1 (get column1 :file))
                              other_table (modifyColumnNames table2 (get column2 :file))
                              table_count (count table)
                              field1 (keyword (str (get column1 :file) "." (get column1 :field)))
                              field2 (keyword (str (get column2 :file) "." (get column2 :field)))]
                          (for [i (range 0 table_count)]
                            (let [elem1 (nth table i)
                                  elem2 (remove nil? (loop [index 0
                                                            limit (count other_table)
                                                            elements []]
                                                       (if (< index limit)
                                                         (recur
                                                           (+' 1 index)
                                                           limit
                                                           (if (= (get elem1 field1)
                                                                  (get (nth other_table index) field2))
                                                             (conj elements (nth other_table index))
                                                             elements))
                                                         elements)))]
                              (if (not= 0 (count elem2))
                                (for [el elem2]
                                  (merge elem1 el))
                                nil)))))))

; ========================================
; Implementation for WHERE query

; checks each line in
(defn check_expression
  [clause data_value]
  ;(println "==================CHECK_EXPRESSION==================")
  (let [data (if (= (type data_value) java.lang.String)
               (str "\"" (lower-case data_value) "\"")
               data_value)]
    (eval (read-string (str
                         "("
                         (get clause :operation)
                         " "
                         data
                         " "
                         (get clause :bound)
                         ")")))))

; checks each line of the file on the conditions mentioned in clause
(defn check_true
  [clause clauseWord selected_element]
  ;(println "==================CHECK_TRUE==================")
  ;(print "clause: ")
  ;(println clause)
  (if (= clauseWord "and")
    (not (contains? (set (for [state clause]
                           (when (= false
                                    (check_expression
                                      (select-keys state [:operation :bound])
                                      (get selected_element (keyword (get state :column)))))
                             -1)))
                    -1))
    (not (= (count clause)
            (count (remove nil? (vec (for [state clause]
                                       (when (= false
                                                (check_expression
                                                  (select-keys state [:operation :bound])
                                                  (get selected_element (keyword (get state :column)))))
                                         -1)))))))))

; file is the result after 'select' query,
; clause has the next structure:
; [
;   possible to have the first element as "and" or "or"
;   [ "column_name" ">=/not=/<=/=" "bound" ]
; ]
(defn where
  [selected_data clause_undone]
  ;(println "======WHERE======")
  (let [clauseWord (get clause_undone :clauseWord)
        clause (get clause_undone :clause)]
    ;(print "clause: ")
    ;(println clause)
    ;(print "clauseWord: ")
    ;(println clauseWord)
    (remove nil? (for [element selected_data]
                   (when (check_true clause clauseWord element) element)))))


; ========================================
; Implementation for SELECT query

(defn select
  [query commands joinClause groupClause havingClause]
  (println "================SELECT===============")
  (let [usedJoin? (some? joinClause)
        usedGroup? (some? groupClause)
        usedHaving? (some? havingClause)
        field1 (if usedJoin?
                 (get joinClause :field1)
                 nil)
        field2 (if usedJoin?
                 (get joinClause :field2)
                 nil)
        file1 (if usedJoin?
                (get joinClause :file1)
                nil)
        file2 (if usedJoin?
                (get joinClause :file2)
                nil)
        data (cond
               ; first condition
               (some #(= "inner join" %) commands)
               ; first action
               (inner-join {:field field1 :file file1} {:field field2 :file file2} (choose_file file1) (choose_file file2))
               ; second condition
               (some #(= "full outer join" %) commands)
               ; second action
               (full-outer-join {:field field1 :file file1} {:field field2 :file file2} (choose_file file1) (choose_file file2))
               ; third condition
               (some #(= "left join" %) commands)
               ; third action
               (left-join {:field field1 :file file1} {:field field2 :file file2} (choose_file file1) (choose_file file2))
               ; else
               :else (choose_file (get query :file)))
        columns_raw (get query :columns)
        usedCase? (some some? (remove false? (for [column columns_raw] (some? (get column :case)))))
        columns_case (filterv #(some? (get % :case)) columns_raw)
        columns (filterv #(nil? (get % :case)) columns_raw)
        columns_list (vec (for [column columns]
                            (let [col_name (if usedJoin?
                                             (str (get column :file)
                                                  "."
                                                  (get column :column))
                                             (get column :column))
                                  col_function (get column :function)]
                              (keyword
                                  (if (some? col_function)
                                    (str col_function
                                         "<"
                                         col_name
                                         ">")
                                    col_name)))))
        having_columns (get havingClause :columns)
        having_columns_usual (if usedHaving?
                               (vec (remove nil? (for [column having_columns]
                                                   (if (nil? (get column :function))
                                                     (if usedJoin?
                                                       (keyword (str (get column :file)
                                                                     "."
                                                                     (get column :column)))
                                                       (keyword (get column :column)))
                                                     nil))))
                              [])
        having_columns_functions (if usedHaving?
                                  (vec (remove nil? (for [column having_columns]
                                                      (if (some? (get column :function))
                                                        column
                                                        nil))))
                                  [])
        columns_usual_vector_raw (vec (remove nil? (for [column columns]
                                                 (if (nil? (get column :function))
                                                   (if usedJoin?
                                                     (keyword (str (get column :file)
                                                                   "."
                                                                   (get column :column)))
                                                     (keyword (get column :column)))
                                                   nil))))
        columns_functions_vector_raw (vec (remove nil? (for [column columns]
                                                     (if (some? (get column :function))
                                                       column
                                                       nil))))
        columns_usual_vector (loop [index 0
                                    limit (count having_columns_usual)
                                    columns_usual columns_usual_vector_raw]
                               (if (< index limit)
                                 (recur
                                   (+' 1 index)
                                   limit
                                   (if (some #(= (nth having_columns_usual index) %) columns_usual)
                                     columns_usual
                                     (conj columns_usual (nth having_columns_usual index))))
                                 columns_usual))
        columns_functions_vector (loop [index 0
                                        limit (count having_columns_functions)
                                        columns_functions columns_functions_vector_raw]
                                   (if (< index limit)
                                     (recur
                                       (+' 1 index)
                                       limit
                                       (if (some #(= (nth having_columns_functions index) %) columns_functions)
                                         columns_functions
                                         (conj columns_functions (nth having_columns_usual index))))
                                     columns_functions))
        groupByDataMap (if usedGroup?
                         (group-by (keyword (if usedJoin?
                                              (str (get groupClause :file)
                                                   "."
                                                   (get groupClause :column))
                                              (get groupClause :column))) data)
                         nil)
        groupedDataKeys (if usedGroup?
                          (keys groupByDataMap)
                          nil)
        select_usual (if usedGroup?
                       (apply merge (for [elem groupedDataKeys]
                                (assoc {} elem (select-keys (first (get groupByDataMap elem)) columns_usual_vector))))
                       (for [line data]
                         (select-keys line columns_usual_vector)))
        select_functions (callFunctions columns_functions_vector
                                        usedJoin?
                                        usedGroup?
                                        groupClause
                                        data)
        resultRaw (if usedGroup?
                    (cond
                      (empty? select_usual) (for [groupDataKey groupedDataKeys]
                                              (get select_functions groupDataKey))
                      :else (let [resultRaw (for [groupDataKey groupedDataKeys]
                                              (merge
                                                (get select_functions groupDataKey)
                                                (get select_usual groupDataKey)))
                                  result (if usedHaving?
                                           (for [line (where resultRaw havingClause)]
                                             (select-keys line columns_list))
                                           resultRaw)]
                              result))
                    (cond
                      (empty? select_functions) select_usual
                      (empty? select_usual) (vector select_functions)
                      :else (let [result (apply merge (first select_usual) (vector select_functions))]
                              (if (= (type result) clojure.lang.PersistentArrayMap)
                                (vector result)
                                result))))
        result (if usedCase?
                 (let [inter_result (for [line resultRaw]
                                      (apply merge
                                             line
                                             (for [col columns_case]
                                               (let [col_name (get col :column)
                                                     whenConditions (get (get col :caseClause) :whenConditions)
                                                     col_list_raw (for [condition whenConditions]
                                                                    (if (check_true (get condition :clause) (get condition :clauseWord) line)
                                                                      (get condition :value)
                                                                      nil))
                                                     col_list_remade (if (some some? col_list_raw)
                                                                       (first (remove nil? col_list_raw))
                                                                       (get (get col :caseClause) :elseCondition))]
                                                 (assoc {} (keyword col_name) col_list_remade)))))]
                   inter_result)
                 resultRaw)]
    result))

; ========================================
; Implementation for SELECT DISTINCT query

(defn select_distinct
  [query commands joinClause groupClause havingClause]
  (vec (set (select query commands joinClause groupClause havingClause))))

(defn orderBy
  [query orderClause]
  (if (not= nil orderClause)
    (sort (eval (read-string orderClause)) query)
    query))

; ========================================
; Implementation for query parsing

; checks if the 'where' query needs to be validated
; then returns the data after validating if needed
(defn checkWhere
  [selected_data commands clause]
  ;(println "=====CHECKWHERE=====")
  ;(print "selected_data: ")
  ;(println selected_data)
  (if (some #(= "where" %) commands)
    (where selected_data clause)
    selected_data))

(defn checkSelect
  [query commands joinClause groupClause havingClause]
  ;(println "==================CHECKSELECT==================")
  (if (some #(= "distinct" %) commands)
    (select_distinct query commands joinClause groupClause havingClause)
    (select query commands joinClause groupClause havingClause)))

(defn printResult
  [query]
  ;(print "query: ")
  ;(println query)
  (clojure.pprint/print-table query))

; starts the execution of the correct function
(defn executeQuery
  [parsed_query]
  ;(println "==================EXECUTEQUERY==================")
  (let [commands (get parsed_query :commands)
        query (get parsed_query :query)
        clause (get parsed_query :clause)
        orderClause (get parsed_query :orderClause)
        joinClause (get parsed_query :joinClause)
        groupClause (get parsed_query :groupClause)
        havingClause (get parsed_query :havingClause)]
    (printResult (orderBy (checkWhere (checkSelect query commands joinClause groupClause havingClause) commands clause) orderClause))))

(defn getGroupClause
  [query_raw]
  ;(println "=====GETGROUPCLAUSE=====")
  (let [column (first query_raw)]
    (let [indexLeft (index-of column "(")
          indexStar (index-of column "*")
          indexDot (index-of column ".")
          group_column (if (some? indexDot)
                         (subs column (+ 1 indexDot))
                         column)
          file_temp (if (some? indexDot)
                      (subs column 0 indexDot)
                      nil)
          function (if (or (some? indexLeft)
                           (some? indexStar))
                     (throw (Exception. ("Agregate functions are not allowed in the GROUP BY query!")))
                     nil)
          result {:column group_column
                  :file file_temp}]
      result
      )))

; parses the join clause to the format:
; (e.g. from "inner join table2 on table1.column1 = table2.column2"
;       to {
;             :field1 "column1"
;             :field2 "column2"
;             :file1 "file1"
;             :file2 "file2"
;          }
(defn getJoinClause
  [query_raw]
  (let [query (subvec query_raw (+ 1 (.indexOf query_raw "on")))
        column_names (vector (first query) (peek query))
        on_columns (vec (for [col column_names]
                          (let [indexDot (index-of col ".")
                                column (subs col (+ 1 indexDot))
                                file (subs col 0 indexDot)]
                            {:column column
                             :file file})))
        first_file (get (first on_columns) :file)
        second_file (get (peek on_columns) :file)
        first_field (get (first on_columns) :column)
        second_field (get (peek on_columns) :column)]
    {:field1 first_field
     :field2 second_field
     :file1 first_file
     :file2 second_file})
  )

; parses the multi conditional clause to the format:
; (e.g. from "mp_id>=21000 and mp_id<=21200"
;       to {
;             :clauseWord "word"
;             :clause [
;                       {
;                         :column "mp_id"
;                         :operation ">="
;                         :bound "21000"
;                       }
;                       {
;                         :column "mp_id"
;                         :operation "<="
;                         :bound "21000"
;                       }
;                     ]
;         }
(defn parseComplexClause
  [clause_undone clauseWord]
  (loop [index 0
         clauseBonds [0]
         clauses []]
    (if (< index (count clause_undone))
      (cond
        (= clauseWord (nth clause_undone index))
        (recur (+ 1 index)
               [(+ 1 index)]
               (conj clauses (subvec clause_undone (first clauseBonds) index)))
        (= index (- (count clause_undone) 1))
        (recur (+ 1 index)
               [(+ 1 index)]
               (conj clauses (subvec clause_undone (first clauseBonds))))
        :else (recur (+ 1 index)
                     clauseBonds
                     clauses))
      clauses))
  )

; parses the simple clause (e.g.: from ["mp_id>=21000"] to { :column "mp_id"
;                                                          :operation ">="
;                                                          :bound "21000" }
;                                 from ["not" "mp_id>=21000"] to { :column "mp_id"
;;                                                             :operation "<="
;;                                                             :bound "21000" })
(defn getSimpleClause
  [clause_undone]
  ;(print "=====GETSIMPLECLAUSE=====")
  (let [clause (if (some #(= "not" %) clause_undone)
                 (clojure.string/join " " (subvec clause_undone 1))
                 (clojure.string/join " " (subvec clause_undone 0)))
        oppositeOperations {">=" "<=", "<>" "=", "=" "<>", "<=" ">=", "<" ">", ">" "<"}
        operationsTranslations {">=" ">=", "<=" "<=", "<>" "not=", "=" "=", "<" "<", ">" ">"}
        operation (first (remove nil? (for [element (keys operationsTranslations)]
                                        (if-not (nil? (clojure.string/index-of clause element)) element))))
        column (clojure.string/replace (clojure.string/replace (subs clause 0 (clojure.string/index-of clause operation)) "(" "<") ")" ">")
        finalOperation (get operationsTranslations (if (some #(= "not" %) clause_undone)
                                                     (get oppositeOperations operation)
                                                     operation))
        bound (str (subs clause (+ (count operation) (clojure.string/index-of clause operation))))]
    ;(println "finished getSimpleClause")
    {:column column
     :operation finalOperation
     :bound (if (and (starts-with? bound "'") (ends-with? bound "'"))
              (clojure.string/replace bound "'" "\"")
              bound)}))

; parses the clause
(defn getClause
  [clause_undone]
  (let [clauseWord (cond
                     (some #(= "and" %) clause_undone)
                     "and"
                     (some #(= "or" %) clause_undone)
                     "or"
                     :else nil)]
    (cond
      (nil? clauseWord) {:clauseWord nil :clause (vector (getSimpleClause clause_undone))}
      (= "and" clauseWord) {:clauseWord "and" :clause (for [element (parseComplexClause clause_undone clauseWord)]
                                                        (getSimpleClause element))}
      (= "or" clauseWord) {:clauseWord "or" :clause (for [element (parseComplexClause clause_undone clauseWord)]
                                                      (getSimpleClause element))}
      )))

; parses count<mps-declarations_rada.mp_id>
(defn parseHavingColumn
  [raw_column file]
  (let [initialRawColumn (clojure.string/trimr (clojure.string/replace (clojure.string/replace raw_column "<" "(") ">" ")"))
        indexLeft (index-of initialRawColumn "(")
        indexRight (index-of initialRawColumn ")")
        indexDot (index-of initialRawColumn ".")
        column (cond
                 (and (some? indexDot)
                      (some? indexRight)) (subs initialRawColumn (+ 1 indexDot) indexRight)
                 (some? indexDot) (subs initialRawColumn (+ 1 indexDot))
                 (some? indexLeft) (subs initialRawColumn (+ 1 indexLeft) indexRight)
                 :else initialRawColumn)
        file_temp (cond
                    (and (some? indexDot)
                         (some? indexLeft)) (subs initialRawColumn (+ 1 indexLeft) indexDot)
                    (some? indexDot) (subs initialRawColumn 0 indexDot)
                    :else file)
        function (if (some? indexLeft)
                   (subs initialRawColumn 0 indexLeft)
                   nil)]
    {:column column
     :function function
     :file file_temp
     :case nil
     :caseClause nil}))

; (getHavingClause ["mp_id" ">=" "21000" "and" "mp_id" "<=" "21200" "and" "count(mp_id)" ">=" "21000"] "temp_file")
; (getHavingClause ["count(mp_id)" ">=" "21000"] "temp_file")
; (getHavingClause ["full_name" ">=" "'a" "b'"] "temp_file")
(defn getHavingClause
  [query_raw file]
  ;(println "=====GETHAVINGCLAUSE=====")
  ;(print "query_raw: ")
  ;(println query_raw)
  (let [clauseWord (cond
                       (some #(= "and" %) query_raw) "and"
                       (some #(= "or" %) query_raw) "or"
                       :else nil)
        parsedClause (if (some? clauseWord)
                       {:clauseWord clauseWord
                        :clause (for [elem (parseComplexClause query_raw clauseWord)]
                                  (getSimpleClause elem))}
                       {:clauseWord nil
                        :clause (vector (getSimpleClause query_raw))})
        parsedColumns (for [elem (:clause parsedClause)]
                        (parseHavingColumn (:column elem) file))]
    ;(print "parsedClause: ")
    ;(println parsedClause)
    ;(print "parsedColumns: ")
    ;(println parsedColumns)
    (assoc parsedClause :columns parsedColumns)))

; mp_id name asc
; mp_id asc name desc
(defn getOrderClause
  [query_raw]
  (let [query_remade (loop [query []
                            lastIndex 0
                            index 0]
                       (if (< index (count query_raw))
                         (cond
                           ; first condition
                           (or (= "asc" (lower-case (nth query_raw index)))
                               (= "desc" (lower-case (nth query_raw index))))
                           ; first action
                           (recur (conj query (apply vector (nth query_raw index) (subvec query_raw lastIndex index)))
                                  (+ 1 index)
                                  (+ 1 index))
                           ; second condition
                           (= index (- (count query_raw) 1))
                           ; second action
                           (recur (conj query (apply vector "asc" (subvec query_raw lastIndex)))
                                  (+ 1 index)
                                  (+ 1 index))
                           :else
                           (recur query
                                  lastIndex
                                  (+ 1 index)))
                         query))
        firstVector (join "" (vector
                               "["
                               (join " " (for [el query_remade]
                                           (join (for [element (rest el)]
                                                   (str
                                                     "(:"
                                                     (clojure.string/replace (clojure.string/replace element "(" "<") ")" ">")
                                                     " "
                                                     (if (= "desc" (first el))
                                                       "%2"
                                                       "%1")
                                                     ")")))))
                               "]"))
        secondVector (join "" (vector
                                "["
                                (join " " (for [el query_remade]
                                            (join (for [element (rest el)]
                                                    (str
                                                      "(:"
                                                      (clojure.string/replace (clojure.string/replace element "(" "<") ")" ">")
                                                      " "
                                                      (if (= "desc" (first el))
                                                        "%1"
                                                        "%2")
                                                      ")")))))
                                "]"))]
    ;(print "firstVector: ")
    ;(print firstVector)
    ;(print "secondVector: ")
    ;(print secondVector)
    (str
      "#(compare "
      (read-string firstVector)
      (read-string secondVector)
      ")")))


; checks if the functions in columns are known
(defn processColumns
  [columns]
  (let [columnsFunctions ["count" "sum" "min" nil]]
    (for [column columns]
      (let [function (get column :function)]
        (if (nil? (some #(= function %) columnsFunctions))
          (throw (Exception. (str "The function " function " is not known!")))
          column)))))

; gets the vector of all 'columns' from the file we're parsing
(defn getColumnsFromStar
  [file]
  ;(println "==================GETCOLUMNSFROMSTAR==================")
  (let [columns_raw (keys (first (choose_file file)))]
    (for [elem columns_raw]
      {:column (name elem)
       :function nil
       :file file
       :case nil
       :caseClause nil})))

(defn getCaseValue
  [query_raw]
  ;(println "=====getCaseValue=====")
  (let [else_raw (join " " query_raw)
        else_result (if (= \' (first else_raw))
                      (clojure.string/replace else_raw "'" "")
                      (eval (read-string else_raw)))]
    else_result))

; caseClause can be of this type:
; :caseClause {
;              {:clauseWord "and"
;               :whenConditions ({:clause {:column "quantity", :operation ">", :bound "30"}, :value "the quantity is greater than 30"}
;                                {:clause {:column "quantity", :operation "=", :bound "30"}, :value "the quantity is 30"})
;               :elseCondition "the quantity is under 30"}
;             }

(defn getCaseWhenClause
  [query_raw]
  ;(println "=====getCaseWhenClause=====")
  (let [temp_vector_for_word (subvec query_raw (.indexOf query_raw "when") (.indexOf query_raw "then"))
        temp_vector (subvec query_raw (.indexOf query_raw "then"))
        whenFirst? (some #(= "when" %) temp_vector)
        clauseWord (cond
                     (some #(= "and" %) temp_vector_for_word) "and"
                     (some #(= "or" %) temp_vector_for_word) "or"
                     :else nil)]
    (remove nil? (apply conj
                        [{:clauseWord clauseWord
                          :clause (if (some? clauseWord)
                                    (for [element (parseComplexClause (subvec query_raw
                                                                              (+ 1 (.indexOf query_raw "when"))
                                                                              (.indexOf query_raw "then"))
                                                                      clauseWord)]
                                      (getSimpleClause element))
                                    (vector (getSimpleClause (subvec query_raw
                                                                     (+ 1 (.indexOf query_raw "when"))
                                                                     (.indexOf query_raw "then")))))
                          :value (getCaseValue (subvec temp_vector
                                                       (+ 1 (.indexOf temp_vector "then"))
                                                       (if whenFirst?
                                                         (.indexOf temp_vector "when")
                                                         (.indexOf temp_vector "else"))))}]
                        (if whenFirst?
                          (getCaseWhenClause (subvec temp_vector (.indexOf temp_vector "when"))))
                        []))))

(defn parseCaseClause
  [query_raw file]
  ;(println "=====parseCaseClause=====")
  (if (and (some #(= "when" %) query_raw)
           (some #(= "then" %) query_raw)
           (some #(= "else" %) query_raw)
           (some #(= "end" %) query_raw)
           (some #(= "as" %) query_raw))
    (let [whenConditions (flatten (getCaseWhenClause (subvec query_raw (+ 1 (.indexOf query_raw "case")) (+ 1 (.indexOf query_raw "else")))))
          elseCondition (getCaseValue (subvec query_raw (+ 1 (.indexOf query_raw "else")) (.indexOf query_raw "end")))
          column_str (parseHavingColumn (nth query_raw (+ 1 (.indexOf query_raw "as"))) file)]
      ;(print "whenConditions: ")
      ;(println whenConditions)
      ;(print "elseCondition: ")
      ;(println elseCondition)
      ;(print "column_str: ")
      ;(println column_str)
      {:column (get column_str :column)
       :function (get column_str :function)
       :file (get column_str :file)
       :case true
       :caseClause {
                    :whenConditions whenConditions
                    :elseCondition elseCondition
                    }})
    (throw (Exception. "Case expression is not correctly built! It should look like \"case when ... then ... (...) else ... end as ...\"!"))))

; parses the columns and functions into
; (
;   {
;     :column "name"
;     :function nil/"name"
;     :file "name"
;   }
; )
(defn getColumns
  [query_raw commands file]
  ;(println "==================GETCOLUMNS==================")
  (let [columns (subvec query_raw
                        (if (some #(= "distinct" %) commands)
                          2
                          1)
                        (.indexOf query_raw "from"))]
    (processColumns (flatten (for [col columns]
                               (if (= 0 (index-of col "case"))
                                 (let [query_case (clojure.string/split col #" ")
                                       query_remade (parseCaseClause query_case file)]
                                   query_remade)
                                 (let [indexLeft (index-of col "(")
                                       indexRight (index-of col ")")
                                       indexDot (index-of col ".")
                                       column (cond
                                                (and (some? indexDot)
                                                     (some? indexRight)) (subs col (+ 1 indexDot) indexRight)
                                                (some? indexDot) (subs col (+ 1 indexDot))
                                                (some? indexLeft) (subs col (+ 1 indexLeft) indexRight)
                                                :else col)
                                       file_temp (cond
                                                   (and (some? indexDot)
                                                        (some? indexLeft)) (subs col (+ 1 indexLeft) indexDot)
                                                   (some? indexDot) (subs col 0 indexDot)
                                                   :else file)
                                       function (if (some? indexLeft)
                                                  (subs col 0 indexLeft)
                                                  nil)]
                                   (if (= "*" column)
                                     (getColumnsFromStar file_temp)
                                     {:column column
                                      :function function
                                      :file file_temp
                                      :case nil
                                      :caseClause nil}))))))))

(defn checkCaseFunction
  [query]
  (if (some #(= "case" %) query)
    (let [indexCase (.indexOf query "case")
          indexAs (.indexOf query "as")
          before (subvec query 0 indexCase)
          after (subvec query (+ 2 indexAs))
          result (apply conj before (join " " (subvec query indexCase (+ 2 indexAs))) after)]
      (checkCaseFunction result))
    query))

; replaces '_type_' 'join' strings in the query to one '_type_ join' string
(defn checkJoinFunction
  [query]
  (let [indexJoin (.indexOf query "join")
        indexFull (.indexOf query "full")
        indexInner (.indexOf query "inner")
        indexLeft (.indexOf query "left")]
    (if (not= -1 indexJoin)
      (if (not= -1 indexFull)
        (let [before (subvec query 0 indexFull)
              after (subvec query (+ 1 indexJoin))]
          (apply conj before "full outer join" after))
        (if (not= -1 indexInner)
          (let [before (subvec query 0 indexInner)
                after (subvec query (+ 1 indexJoin))]
            (apply conj before "inner join" after))
          (let [before (subvec query 0 indexLeft)
                after (subvec query (+ 1 indexJoin))]
            (apply conj before "left join" after)))
        )
      query)))

; replaces '_command_' 'by' strings in the query to one '_command_ by' string
(defn checkByFunction
  [query word]
  ;(println "================CHECKBYFUNCTION=================")
  (let [mainWord (first (split word #" "))]
    (if (some #(= mainWord %) query)
      (let [raw (assoc query (.indexOf query mainWord) word)]
        (apply conj (subvec raw 0 (.indexOf raw "by")) (subvec raw (+ 1 (.indexOf raw "by")))))
      query)))

; gets those commands from the commands_list in the vector,
; which are present in the query
(defn getCommands
  [query commands_list]
  (filterv (fn [x] (if (some #(= (clojure.string/lower-case x) %) commands_list)
                     true
                     false))
           query))

; checks if the input equals 'exit', if so exits with the code 0
; else, it changes the separate commands '_command_' 'by' to '_command_ by'
;              and the separate commands '_type_' 'join' to '_type_ join'
(defn joinSeparateCommands
  [query_raw_raw]
  (if (= "exit" (first query_raw_raw))
    (System/exit 0)
    (-> (checkByFunction query_raw_raw "order by")
        (checkByFunction "group by")
        (checkJoinFunction)
        (checkCaseFunction))))

; parses the query in the format:
; { :commands ["command1 (e.g. select)" "command2 (e.g. from)" ...]
;   :query     {:file "file_name"
;               :query [
;                         :column "column1"
;                         :function "function1"
;                         :file "file1"
;                      ]
;               }
;   :clause   {
;             :clauseWord "word"
;             :clause [
;                       {
;                         :column "mp_id"
;                         :operation ">="
;                         :bound "21000"
;                       }
;                       {
;                         :column "mp_id"
;                         :operation "<="
;                         :bound "21000"
;                       }
;                     ]
;              }
;   :orderClause "#(compare [(:col1 %1) (:col2 %2)]
;                           [(:col1 %2) (:col2 %1)])"
; }
(defn parseQuery
  [query_raw_raw commands_list]
  (let [usedCase? (some #(= "case" %) query_raw_raw)
        query_raw (joinSeparateCommands query_raw_raw)
        commands_raw (getCommands query_raw commands_list)
        commands (if usedCase?
                   (conj commands_raw "case")
                   commands_raw)
        file (nth query_raw (+ 1 (.indexOf query_raw "from")))
        columns (getColumns query_raw commands file)
        query {:file file
               :columns columns}
        clause (if (some #(= "where" %) commands)
                 (cond
                   ; first condition
                   (and (not (some #(= "order by" %) commands))
                        (not (some #(= "group by" %) commands)))
                   ; first action
                   (getClause
                     (subvec query_raw (+ 1 (.indexOf query_raw "where"))))
                   ; second condition
                   (and (some #(= "order by" %) commands)
                        (not (some #(= "group by" %) commands)))
                   ; second action
                   (getClause
                     (subvec query_raw (+ 1 (.indexOf query_raw "where")) (.indexOf query_raw "order by")))
                   ; third condition
                   (some #(= "group by" %) commands)
                   ; third action
                   (getClause
                     (subvec query_raw (+ 1 (.indexOf query_raw "where")) (.indexOf query_raw "group by")))
                   ; else condition and action
                   :else nil
                   )
                 nil)
        orderClause (cond
                      (some #(= "order by" %) commands)
                      (getOrderClause (subvec query_raw (+ 1 (.indexOf query_raw "order by"))))
                      :else nil)
        joinClause (if (or (some #(= "inner join" %) commands)
                           (some #(= "full outer join" %) commands)
                           (some #(= "left join" %) commands))
                     (cond
                       ; first condition
                       (some #(= "where" %) commands)
                       (getJoinClause (subvec query_raw (+ 2 (.indexOf query_raw "from")) (.indexOf query_raw "where")))
                       ; second condition
                       (and (not (some #(= "where" %) commands))
                            (some #(= "group by" %) commands))
                       (getJoinClause (subvec query_raw (+ 2 (.indexOf query_raw "from")) (.indexOf query_raw "group by")))
                       ; third condition
                       (and (not (some #(= "where" %) commands))
                            (not (some #(= "group by" %) commands))
                            (some #(= "order by" %) commands))
                       (getJoinClause (subvec query_raw (+ 2 (.indexOf query_raw "from")) (.indexOf query_raw "order by")))
                       ; third condition and action
                       :else (getJoinClause (subvec query_raw (+ 2 (.indexOf query_raw "from"))))
                       )
                     nil)
        groupClause (if (some #(= "group by" %) commands)
                      (cond
                        ; first condition
                        (some #(= "having" %) commands)
                        (getGroupClause (subvec query_raw (+ 1 (.indexOf query_raw "group by")) (.indexOf query_raw "having")))
                        ; second condition
                        (and (some #(= "order by" %) commands)
                             (not (some #(= "having" %) commands)))
                        (getGroupClause (subvec query_raw (+ 1 (.indexOf query_raw "group by")) (.indexOf query_raw "order by")))
                        ; third condition
                        :else (getGroupClause (subvec query_raw (+ 1 (.indexOf query_raw "group by"))))
                        )
                      nil)
        havingClause (if (and (some? groupClause)
                              (some #(= "having" %) commands))
                       (cond
                         ; first condition
                         (some #(= "order by" %) commands)
                         (getHavingClause (subvec query_raw (+ 1 (.indexOf query_raw "having")) (.indexOf query_raw "order by")) file)
                         ; second condition
                         :else (getHavingClause (subvec query_raw (+ 1 (.indexOf query_raw "having"))) file)
                         )
                       nil)]
    (print "commands: ")
    (println commands)
    (print "query: ")
    (println query)
    (print "clause: ")
    (println clause)
    (print "orderClause: ")
    (println orderClause)
    (print "joinClause: ")
    (println joinClause)
    (print "groupClause: ")
    (println groupClause)
    (print "havingClause: ")
    (println havingClause)
    {:commands commands
     :query query
     :clause clause
     :orderClause orderClause
     :joinClause joinClause
     :groupClause groupClause
     :havingClause havingClause}))
; yet to add the JOIN query

(defn -main [& args]
  (println "Write your commands here!")
  (print "~> ")
  (flush)
  (def commands_list ["select" "from" "where" "distinct" "order by" "group by" "inner join" "full outer join" "left join" "on" "having" "case"])
  (loop [input (read-line)]
    (executeQuery (parseQuery (clojure.string/split (clojure.string/lower-case (clojure.string/replace input #"[,;]" "")) #" ") commands_list)))
  (recur (-main)))

; COMMANDS

; on WHERE, SELECT, DISTINCT, ORDER BY
; select distinct mp_id, full_name from mp-posts_full where not mp_id>=21100;
; select distinct mp_id, full_name from mp-posts_full where mp_id>=21900 order by mp_id;
; select distinct mp_id, full_name from mp-posts_full where mp_id>=21100;
; select distinct mp_id, full_name from mps-declarations_rada;
; select distinct mp_id from mp-posts_full order by mp_id desc;
; select distinct mp_id, full_name from mp-posts_full where mp_id>=21500 or mp_id<=5000;
; select distinct row, col from map_zal-skl9 where row>=2 and row<=7;
; select distinct mp_id, full_name from mp-posts_full where not mp_id<>21111;
; select distinct mp_id from mp-posts_full where not mp_id<>21111;
; select distinct row, col from map_zal-skl9 where row>=12 and row<=13 order by row, col;
; select distinct row, col from map_zal-skl9 where row>=12 and row<=13 order by row asc, col desc;
; select distinct * from map_zal-skl9 where row>=12 and row<=13 order by row asc, col desc;
; select distinct mp_id, full_name from mp-posts_full order by mp_id;

; on ~, NOT, " ...<>'abc' "
; select distinct mp_id, full_name from mp-posts_full where not full_name<>'Яцик Юлія Григорівна';
; select distinct mp_id, full_name from mp-posts_full where not full_name<>'Яцик Юлія Григорівна' or not full_name<>'Яцик Юлія Григорівна';
; select distinct mp_id, full_name from mp-posts_full where not full_name<>'Яцик Юлія Григорівна' or full_name='Заремський Максим Валентинович';
; select distinct mp_id, full_name from mp-posts_full where not full_name<>'Яцик Юлія Григорівна' or full_name='Заремський Максим Валентинович' or mp_id>=21200;
; select distinct mp_id, full_name from mp-posts_full where not full_name='Яцик Юлія Григорівна' and not full_name='Заремський Максим Валентинович' and mp_id>=21052 and mp_id<=21102;
; select distinct mp_id, full_name from mp-posts_full where not full_name<>'Яцик Юлія Григорівна' and mp_id>=21052 and mp_id<=21056;

; on ~, functions, functions + non-functions
; select mp_id, full_name, count(mp_id), count(full_name) from mp-posts_full order by mp_id asc;
; select count(mp_id) from mp-posts_full;
; select count(mp_id), count(full_name) from mp-posts_full;

; on ~, inner join, functions + inner join
; select mps-declarations_rada.mp_id from mps-declarations_rada inner join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id;
; select distinct Count(mps-declarations_rada.mp_id), mp-posts_full.full_name from mps-declarations_rada inner join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id;
; select distinct mps-declarations_rada.mp_id, mp-posts_full.full_name from mps-declarations_rada inner join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id;
; select distinct mps-declarations_rada.mp_id, mp-posts_full.full_name from mps-declarations_rada inner join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id order by mps-declarations_rada.mp_id;
; select distinct map_zal-skl9.id_mp, mp-posts_full.mp_id from map_zal-skl9 inner join mp-posts_full on map_zal-skl9.id_mp = mp-posts_full.mp_id order by map_zal.skl9.id_mp desc;
; select mps-declarations_rada.mp_id from mps-declarations_rada inner join mp-posts_full on mps-declarations_rada.fullname = mp-posts_full.full_name;
; select distinct mps-declarations_rada.mp_id from mps-declarations_rada inner join mp-posts_full on mps-declarations_rada.fullname = mp-posts_full.full_name;
; select mps-declarations_rada.* from mps-declarations_rada inner join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id;
; select mps-declarations_rada.*, mp-posts_full.* from mps-declarations_rada inner join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id;

; on ~, full outer join, functions + full outer join (change to left join if you want)
; select distinct map_zal-skl9.id_mp, mp-posts_full.mp_id from map_zal-skl9 full outer join mp-posts_full on map_zal-skl9.id_mp = mp-posts_full.mp_id order by map_zal-skl9.id_mp desc, mp-posts_full.mp_id desc;
; select mps-declarations_rada.mp_id from mps-declarations_rada full outer join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id;
; select distinct Count(mps-declarations_rada.mp_id), mp-posts_full.full_name from mps-declarations_rada full outer join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id;
; select distinct mps-declarations_rada.mp_id, mp-posts_full.full_name from mps-declarations_rada full outer join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id;
; select distinct mps-declarations_rada.mp_id, mp-posts_full.full_name from mps-declarations_rada full outer join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id order by mps-declarations_rada.mp_id;
; select distinct mps-declarations_rada.mp_id, mp-posts_full.full_name from mps-declarations_rada full outer join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id where mps-declarations_rada.mp_id=15816;
; select distinct mps-declarations_rada.mp_id, mp-posts_full.full_name from mps-declarations_rada full outer join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id where mps-declarations_rada.mp_id=15816 order by mp-posts_full.full_name;

; on ~, group by
; select Count(mp_id), full_name from mp-posts_full group by full_name;
; ERROR -> select mp_id, full_name from mp-posts_full group by full_name;
; select Count(mp_id), Sum(mp_id), full_name from mp-posts_full where Count(mp_id)>=10 group by full_name;
; select Count(mp_id), full_name from mp-posts_full where Count(mp_id)>=10 group by full_name;
; select Count(mp_id), Sum(mp_id), full_name from mp-posts_full group by full_name order by full_name;

; on ~, having
; select Count(mp_id), full_name from mp-posts_full group by full_name having Count(mp_id)<11;
; select Count(mp_id), full_name from mp-posts_full group by full_name having full_name='Іванов Володимир Ілліч';
; select Count(mp_id), Sum(mp_id), full_name from mp-posts_full group by full_name having full_name='Іванов Володимир Ілліч' or Sum(mp_id)<3584 order by Count(mp_id);
; select Count(mp_id), Sum(mp_id), full_name from mp-posts_full group by full_name order by Sum(mp_id);
; INNER JOIN + GROUP BY
; select Count(mps-declarations_rada.mp_id), mp-posts_full.full_name from mps-declarations_rada inner join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id group by mp-posts_full.full_name;
; THE SAME + HAVING
; select Count(mps-declarations_rada.mp_id), mp-posts_full.full_name from mps-declarations_rada inner join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id group by mp-posts_full.full_name having Count(mps-declarations_rada.mp_id)<=21;

; on ~, case
; THE NEXT ONE WITH SURPRISE WITH COUNTING
; select distinct mp_id, case when mp_id<2000 and mp_id>1000 then 'Lesser than 2000 and bigger than 1000' when mp_id<10000 then 'Between 2000 and 10000' when mp_id<20000 then 'Between 10000 and 20000' else 'Bigger than 20000' end as Boundary from mp-posts_full order by mp_id;
; select distinct mp_id, case when mp_id<2000 then 'Less than 2000' when mp_id<10000 then 'Between 2000 and 10000' when mp_id<20000 then 'Between 10000 and 20000' else 'Bigger than 20000' end as Boundary from mp-posts_full order by mp_id;
; select distinct mp_id, case when mp_id<10000 then 'Lesser than 10000' else 'Bigger than 10000' end as Boundary from mp-posts_full order by mp_id;
; CASE + INNER JOIN
; select distinct mps-declarations_rada.mp_id, mp-posts_full.full_name, case when mps-declarations_rada.mp_id<10000 then 'Lesser than 10000' else 'Bigger or equal than 10000' end as boundary from mps-declarations_rada inner join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id;
; CASE + INNER JOIN + GROUP BY
; select Count(mps-declarations_rada.mp_id), mp-posts_full.full_name, case when Count(mps-declarations_rada.mp_id)<=20 then 'lesser than 20' else 'bigger or equal than 20' end as boundary from mps-declarations_rada inner join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id group by mp-posts_full.full_name order by Count(mps-declarations_rada.mp_id);
; ~ + more cases
; select Count(mps-declarations_rada.mp_id), mp-posts_full.full_name, case when Count(mps-declarations_rada.mp_id)<=20 then 'lesser than 20' else 'bigger or equal than 20' end as boundary, case when Count(mps-declarations_rada.mp_id)<=20 then 'bigger than 20' else 'lesser or equal than 20' end as boundary_reverse from mps-declarations_rada inner join mp-posts_full on mps-declarations_rada.mp_id = mp-posts_full.mp_id group by mp-posts_full.full_name order by Count(mps-declarations_rada.mp_id);
