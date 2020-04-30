(ns lab6.core
  (:gen-class)
  (:use [clojure.string]
        [clojure.data.csv]
        [clojure.data.json])
  (:import (clojure.lang SeqEnumeration)))

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
(def mp-posts_full
  (loadFile "../data files/mp-posts_full.csv"))
(def map_zal-skl9
  (loadFile "../data files/map_zal-skl9.csv"))
(def plenary_register_mps-skl9
  (loadFile "../data files/plenary_register_mps-skl9.tsv"))

; ADDITIONAL
(def plenary_vote_results-skl9
  (loadFile "../data files/plenary_vote_results-skl9.tsv"))

; EXTRA
(def mps-declarations_rada
  (loadFile "../data files/mps-declarations_rada.json"))

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
; Implementation for SELECT query

(defn isdigit?
  [character]
  (if (nil? character)
    false
    (if (and (>= (int character) 48) (<= (int character) 57))
      true
      false)))

(defn Min
  [file columns col_index]
  (def column (for [line file]
                (nth line col_index)))
  (print "column: ")
  (println column)
  (loop [index 0
         limit (count column)
         is_string (if (isdigit? (ffirst column))
                     false
                     true)
         min (if (= true is_string)
               (first column)
               (read-string (first column))
               )]
    (if (< index limit)
      (recur (+' 1 index)
             limit
             is_string
             (if (= true is_string)
               (if (< (count (nth column index)) (count min))
                 (nth column index)
                 min)
               (if (< (read-string (nth column index)) min)
                 (read-string (nth column index))
                 min)))
      (vector (str min))))
  )

(defn Sum
  [file col_index]
  (println "===============SUM==============")
  (print "file: ")
  (println file)
  (print "col index: ")
  (println col_index)
  (print "")
  (if (isdigit? (first (nth (first file) col_index)))
    (loop [index 0
           limit (count file)
           sum 0
           col col_index]
      (if (< index limit)
        (recur (+' 1 index)
               limit
               (+' (eval (read-string (nth (nth file index) col))) sum)
               col)
        (vector (str sum))))
    (throw (Exception. "The values in the 'SUM' function are not digits!"))))

(defn Count
  [file column]
  (print "type of a count: ")
  (println (type (count file)))
  (vector (str (count file))))

(defn callFunction
  [file column index columns]
  (println "CALLFUNCTION")
  (print "columns: ")
  (println columns)
  (print "The result of a count: ")
  (println (Count file (peek column)))
  (case (first column)
    "count" (Count file (peek column))
    "sum" (Sum file index)
    "min" (Min file columns index)
    ))

; select count(mp_id) from mp-posts_full;

(defn callFunctions
  [file columns]
  (println "==============CALLFUNCTIONS=============")
  (println "file: ")
  (println file)
  (print "columns: ")
  (println columns)
  (println (for [column columns]
             (when (not= "" (first column))
               (callFunction file column (.indexOf columns column) columns))))
  (for [column columns]
    (when (not= "" (first column))
      (callFunction file column (.indexOf columns column) columns))))

(defn select
  [query commands]
  (println "================SELECT===============")
  (def temp (let [[file & columns] query]
              (print "file: ")
              (println file)
              (print "columns: ")
              (println columns)
              (print "amount of the functions: ")
              (println (count (remove nil? (for [column columns]
                                             (if (not= "" (first column))
                                               (first column)
                                               nil)))))))
  (let [[file & columns] query]
    (cond
      (and (= 0 (count (remove nil? (for [column columns]
                                      (if (not= "" (first column))
                                        (first column)
                                        nil)))))
           (= -1 (.indexOf commands "group by")))
      (for [element (choose_file file)]
        (for [column (vec columns)]
          (get element (keyword (peek column)))))
      (= -1 (.indexOf commands "group by"))
      (callFunctions (for [element (choose_file file)]
                       (for [column (vec columns)]
                         (get element (keyword (peek column)))))
                     columns)
      :else nil
      )))
;do changes to prepare data

; ========================================
; Implementation for SELECT DISTINCT query

(defn select_distinct
  [query commands]
  (vec (set (select query commands))))

; ========================================
; Implementation for WHERE query

; checks each line in
(defn check_expression
  [state data_raw]
  ;(println "==================CHECK_EXPRESSION==================")
  (def data (if (isdigit? (first data_raw))
              data_raw
              (str "\"" (lower-case data_raw) "\"")))
  ;(print "data: ")
  ;(println data)
  ;(print "type of data: ")
  ;(println (type data))
  ;(print "state: ")
  ;(println state)
  (eval (read-string (clojure.string/join " " (vector
                                                "("
                                                (first state)
                                                data
                                                (nth state 1)
                                                ")")))))

; select distinct mp_id, full_name from mp-posts_full where mp_id>=21500 or mp_id<=5000;
; select distinct row, col from map_zal-skl9 where row>=2 and row<=5;
; select distinct mp_id, full_name from mp-posts_full where not full_name<>'Яцик Юлія Григорівна' or full_name='Заремський Максим Валентинович' or mp_id>=21200;
; select distinct mp_id, full_name from mp-posts_full where not mp_id<>21111;
; select distinct mp_id, full_name from mp-posts_full where not full_name='Яцик Юлія Григорівна' and not full_name='Заремський Максим Валентинович' and mp_id>=21052 and mp_id<=21102;
; select distinct mp_id, full_name from mp-posts_full where not full_name='Яцик Юлія Григорівна' and mp_id>=21052 and mp_id<=21056;
; select distinct row, col from map_zal-skl9 where row>=12 and row<=13 order by row, col;
; select distinct row, col from map_zal-skl9 where row>=12 and row<=13 order by row asc, col desc;

; checks each line of the file on the conditions mentioned in clause
(defn check_true
  [clause clauseWord data]
  ;(println "==================CHECK_TRUE==================")
  ;(print "clause: ")
  ;(println clause)
  (if (= clauseWord "and")
    (not (contains? (set (for [state clause]
                           (when (= false
                                    (check_expression
                                      (vec (rest state))
                                      (nth data (read-string (first state)))))
                             -1))) -1))
    (not (= (count clause) (count (remove nil? (vec (for [state clause]
                                                      (when (= false
                                                               (check_expression
                                                                 (vec (rest state))
                                                                 (nth data (read-string (first state)))))
                                                        -1)))))))))

; file is the result after 'select' query,
; clause has the next structure:
; [
;   possible to have the first element as "and" or "or"
;   [ "number_of_column" ">=/not=/<=/=" "bound" ]
; ]
(defn where
  [file clause_undone]
  (println "===============WHERE==============")
  (print "file: ")
  (println file)
  (print "clause_undone: ")
  (println clause_undone)
  (def clauseWord (cond
                    (not= -1 (.indexOf clause_undone "and")) "and"
                    (not= -1 (.indexOf clause_undone "or")) "or"
                    :else nil
                    ))
  (print "clauseWord: ")
  (println clauseWord)
  (def clause (if (not= nil clauseWord)
                (vec (rest clause_undone))
                clause_undone))
  (println "clause: ")
  (println clause)
  (remove nil? (vec
                 (for [line file]
                   (when (check_true clause clauseWord line) (vec line))))))

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
  [select_result commands clause]
  (println "==================CHECKWHERE==================")
  (print "commands: ")
  (println commands)
  (print "query: ")
  (println select_result)
  (print "clause: ")
  (println clause)
  (if (not= -1 (.indexOf commands "where"))
    (where select_result clause)
    select_result))

(defn checkSelect
  [query commands]
  (println "==================CHECKSELECT==================")
  (print "commands: ")
  (println commands)
  (print "query: ")
  (println query)
  (if (not= -1 (.indexOf commands "distinct"))
    (select_distinct query commands)
    (select query commands)))

(defn printResult
  [query]
  (clojure.pprint/print-table query))

; select count(mp_id) from mp-posts_full;
; select count(mp_id), count(full_name) from mp-posts_full;

(defn checkJson
  [file]
  (case file
    "mp-posts_full" false
    "map_zal-skl9" false
    "plenary_register_mps-skl9" false
    "plenary_vote_results-skl9" false
    "mps-declarations_rada" true
    ))


; select distinct mp_id from mps-declarations_rada;
; select distinct mp_id from mps-declarations_rada;

; turns the stringed numbers into normal numbers, then
; makes a map out of the result data to feed it into order by and, then, printResult
(defn prepareData
  [query columns commands file]
  (println "===================PREPAREDATA===================")
  (print "columns: ")
  (println columns)
  (print "query: ")
  (println query)
  (print "types of data in query: ")
  (println (for [element (first query)]
             (type element)))
  (def query_remade (if (checkJson file)
                      query
                      (vec (for [line query]
                             (vec (for [element line]
                                    (if (isdigit? (first element))
                                      (read-string element)
                                      element)))))))

  (print "query_remade: ")
  (println query_remade)
  (print "types of data in query_remade: ")
  (println (for [element (first query_remade)]
             (type element)))
  (def columns_usual (vec (for [column columns]
                            (peek column))))
  (print "columns_usual: ")
  (println columns_usual)
  (def columns_function (vec (for [column columns]
                               (str (first column)
                                    "("
                                    (peek column)
                                    ")"))))
  (print "columns_function: ")
  (println columns_function)
  (print "possible data for functions: ")
  (println (apply vector (for [element (flatten query_remade)]
                           (str element))))
  (print "the types in the possible data for functions: ")
  (println (for [element (apply vector (for [element (flatten query_remade)]
                                         (str element)))]
             (type element)))
  (print "the resulting map: ")
  (println (apply vector columns_function (apply vector (for [element (flatten query_remade)]
                                                          (str element)))))
  (cond
    (and (= -1 (.indexOf commands "group by"))
         (= 0 (count (remove nil? (for [column columns]
                                    (if (not= "" (first column))
                                      (first column)
                                      nil))))))
    (apply mapData (apply vector columns_usual query_remade))
    (= -1 (.indexOf commands "group by"))
    (apply mapData (vector columns_function (apply vector (for [element (flatten query_remade)]
                                                            (str element)))))
    :else nil)
  )

; select count(mp_id), count(full_name) from mp-posts_full;


; starts the execution of the correct function
(defn executeQuery
  [parsed_query]
  (println "==================EXECUTEQUERY==================")
  (let [commands (nth parsed_query 0)
        query (nth parsed_query 1)
        file (nth query 0)
        clause (if (nth parsed_query 2)
                 (nth parsed_query 2)
                 nil)
        orderClause (if (nth parsed_query 3)
                      (nth parsed_query 3)
                      nil)
        columns (vec (rest query))]
    (print "commands: ")
    (println commands)
    (print "query: ")
    (println query)
    (print "clause: ")
    (println clause)
    (print "columns: ")
    (println columns)
    (printResult (orderBy (prepareData (checkWhere (checkSelect query commands) commands clause) columns commands file) orderClause))
    ))

; select distinct mp_id, full_name from mp-posts_full where mp_id>=21200 or mp_id<=9000;
; select distinct mp_id, full_name from mp-posts_full where mp_id>=21200 or not full_name<>'Яцик Юлія Григорівна';

; parses the multi conditional clause to the format:
; (e.g. from "mp_id>=21000 and mp_id<=21200"
;       to [
;           "and"
;           [ "0" ">=" "21000"]
;           [ "0" "<=" "21000"]
;          ]
(defn parseComplexClause
  [clause_undone clauseWord]
  ;(println "==================parseComplexClause==================")
  ;(print "clause_undone: ")
  ;(println clause_undone)
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

; select distinct mp_id, full_name from mp-posts_full where not mp_id>=21100;
; select distinct mp_id, full_name from mp-posts_full where mp_id>=21900 order by mp_id;
; select distinct mp_id, full_name from mp-posts_full where mp_id>=21100;
; select distinct mp_id, full_name from mp-posts_full where not full_name<>'Яцик Юлія Григорівна';
; select distinct mp_id, full_name from mp-posts_full where not full_name<>'Яцик Юлія Григорівна' or not full_name<>'Яцик Юлія Григорівна';
; select distinct mp_id, full_name from mps-declarations_rada;

; parses the simple clause (e.g.: from "mp_id>=21000" to [ "mp_id" ">=" "21000" ]
;                          from "not mp_id>=21000" to [ "mp_id" "<=" "21000" ])
(defn getSimpleClause
  [clause_undone columns_raw]
  (println "==================getSimpleClause==================")
  (print "clause_undone: ")
  (println clause_undone)
  (def columns (vec (for [element columns_raw]
                      (if (not= "" (first element))
                        (str (first element)
                             "("
                             (peek element)
                             ")")
                        (peek element)))))
  (print "columns: ")
  (println columns)
  (def clause (if (not= -1 (.indexOf clause_undone "not"))
                (clojure.string/join " " (subvec clause_undone 1))
                (clojure.string/join " " (subvec clause_undone 0))))
  (print "clause: ")
  (println clause)
  ;
  (def oppositeOperations {">=" "<=", "<>" "=", "=" "<>", "<=" ">="})
  (def operationsTranslations {">=" ">=", "<=" "<=", "<>" "not=", "=" "="})
  ;
  (def operation (first (remove nil? (for [element (keys operationsTranslations)]
                                       (if-not (nil? (clojure.string/index-of clause element)) element)))))
  (print "operation: ")
  (println operation)
  ;
  (def column (str (.indexOf columns (subs clause 0 (clojure.string/index-of clause operation)))))
  (def finalOperation (get operationsTranslations (if (not= -1 (.indexOf clause_undone "not"))
                                                    (get oppositeOperations operation)
                                                    operation)))
  (def bound (str (subs clause (+ (count operation) (clojure.string/index-of clause operation)))))
  ;
  (vector column
          finalOperation
          (if (and (starts-with? bound "'") (ends-with? bound "'"))
            (clojure.string/replace bound "'" "\"")
            bound)))

; parses the clause
(defn getClause
  [clause_undone columns]
  (println "==================GETCLAUSE==================")
  (print "clause undone: ")
  (println clause_undone)
  (print "columns: ")
  (println columns)
  ;
  (def clauseWord (cond
                    (not= -1 (.indexOf clause_undone "and"))
                    "and"
                    (not= -1 (.indexOf clause_undone "or"))
                    "or"
                    :else nil))
  ;
  (cond
    (nil? clauseWord) (vector (getSimpleClause clause_undone columns))
    (= "and" clauseWord) (apply vector "and" (for [element (parseComplexClause clause_undone clauseWord)]
                                               (getSimpleClause element columns)))
    (= "or" clauseWord) (apply vector "or" (for [element (parseComplexClause clause_undone clauseWord)]
                                             (getSimpleClause element columns)))
    )
  )

; gets those commands from the commands_list in the vector,
; which are present in the query
(defn getCommands
  [query commands_list]
  (filterv (fn [x] (if (not= -1 (.indexOf commands_list (clojure.string/lower-case x)))
                     true
                     false))
           query))

; mp_id name asc
; mp_id asc name desc
(defn getOrderClause
  [query_raw]
  ;(println "==================GETORDERCLAUSE==================")
  (def query_remade (loop [query []
                           lastIndex 0
                           index 0]
                      (if (< index (count query_raw))
                        (cond
                          (or (= "asc" (lower-case (nth query_raw index)))
                              (= "desc" (lower-case (nth query_raw index))))
                          (recur (conj query (apply vector (nth query_raw index) (subvec query_raw lastIndex index)))
                                 (+ 1 index)
                                 (+ 1 index))
                          (= index (- (count query_raw) 1))
                          (recur (conj query (apply vector "asc" (subvec query_raw lastIndex)))
                                 (+ 1 index)
                                 (+ 1 index))
                          :else
                          (recur query
                                 lastIndex
                                 (+ 1 index)))
                        query)))
  ;(print "query: ")
  ;(println query_remade)
  ;
  (def firstVector
    (join "" (vector
               "["
               (join " " (for [el query_remade]
                           (join (for [element (rest el)]
                                   (str
                                     "(:"
                                     element
                                     " "
                                     (if (= "desc" (first el))
                                       "%2"
                                       "%1")
                                     ")")))))
               "]")))
  ;(print "first: ")
  ;(println firstVector)
  (def secondVector
    (join "" (vector
               "["
               (join " " (for [el query_remade]
                           (join (for [element (rest el)]
                                   (str
                                     "(:"
                                     element
                                     " "
                                     (if (= "desc" (first el))
                                       "%1"
                                       "%2")
                                     ")")))))
               "]")))
  ;(print "second: ")
  ;(println secondVector)
  (str
    "#(compare "
    (read-string firstVector)
    (read-string secondVector)
    ")")
  )

; select distinct * from map_zal-skl9 where row>=12 and row<=13 order by row asc, col desc;
; select distinct row, col from map_zal-skl9 where row>=12 and row<=13 order by row asc, col desc;


; gets the vector of all 'columns' from the file we're parsing
(defn getColumnsFromStar
  [file]
  ;(println "==================GETCOLUMNSFROMSTAR==================")
  ;(print "file: ")
  ;(println file)
  (def columns (loop [x 0
                      result []]
                 (if (< x (count (keys (first (choose_file (first file))))))
                   (recur (+ x 1)
                          (conj result (name (nth (keys (first (choose_file (first file)))) x))))
                   result)))
  ;(print "columns: ")
  ;(println columns)
  ;(println "==================")
  (vec (for [el columns]
         (vector "" el))))

; select distinct mp_id, full_name from mp-posts_full order by mp_id;

; parses the columns in a format:
; [
;   the first element of the vector is the function if we are using any
;   for that column
;   ["count" "column1"]
;   ["" "column2"]
;   ["sum" "column3"]
;   ...
; ]
(defn checkFunctions
  [query file commands]
  (def query_commands ["count" "sum" "min"])
  (def final_query (apply vector (vec (for [column query]
                                        (cond
                                          (= "*" column)
                                          (getColumnsFromStar file)
                                          (and (not (nil? (clojure.string/index-of column "(")))
                                               (not (nil? (clojure.string/index-of column ")"))))
                                          (clojure.string/split column #"[()]")
                                          :else
                                          (vector "" column))))))
  (print "final query: ")
  (println final_query)
  (def result (if (= clojure.lang.PersistentVector (type (first (first final_query))))
                (first final_query)
                final_query))
  (print "result: ")
  (println result)
  (print "the exception will be thrown: ")
  (println (and
             (= -1 (.indexOf commands "group by"))
             (not= 0 (count (remove nil? (for [column result]
                                           (if (not= "" (first column))
                                             (first column)
                                             nil)))))
             (not= (count result) (count (remove nil? (for [column result]
                                                        (if (not= "" (first column))
                                                          (first column)
                                                          nil)))))))
  (print "result: ")
  (println result)
  (print "the amount of columns: ")
  (println (count result))
  (print "the amount of functions: ")
  (println (count (remove nil? (for [column result]
                                 (if (not= "" (first column))
                                   (first column)
                                   nil)))))


  ; select count(mp_id), count(full_name) from mp-posts_full;

  (cond
    (and
      (= -1 (.indexOf commands "group by"))
      (not= 0 (count (remove nil? (for [column result]
                                    (if (not= "" (first column))
                                      (first column)
                                      nil)))))
      (not= (count result) (count (remove nil? (for [column result]
                                                 (if (not= "" (first column))
                                                   (first column)
                                                   nil))))))
    (throw (Exception. "The amount of the functions not equal to the total amount of columns!"))
    :else result)
  )

; parses the columns and functions
(defn getColumns
  [query_raw commands file]
  (println "==================GETCOLUMNS==================")
  (def query (subvec query_raw
                     (if (not= -1 (.indexOf commands "distinct"))
                       2
                       1)
                     (.indexOf query_raw "from")))
  (print "query: ")
  (println query)
  (checkFunctions query file commands)
  )

; replaces '_command_' 'by' strings in the query to one '_command_ by' string
(defn checkByFunction
  [query word]
  ;(println "================CHECKBYFUNCTION=================")
  (def mainWord (first (split word #" ")))
  (if (not= -1 (.indexOf query mainWord))
    (let [raw (assoc query (.indexOf query mainWord) word)]
      (apply conj (subvec raw 0 (.indexOf raw "by")) (subvec raw (+ 1 (.indexOf raw "by")))))
    query))

; checks if the input equals 'exit', if so exits with the code 0
; else, it changes the separate commands '_command_' 'by' to '_command_ by'
(defn processQuery
  [query_raw_raw]
  (if (= "exit" (first query_raw_raw))
    (System/exit 0)
    (-> (checkByFunction query_raw_raw "order by")
        (checkByFunction "group by"))))

; parses the query in the format:
; [ commands: ["command1 (e.g. select)" "command2 (e.g. from)" ...]
;   query:    ["file_name" "column1" "column2" ...]
;   clause:   [
;               here we put either "and" or "or" or nothing if there is only one condition
;               ["index_of_column" ">=/not=" "bound"]
;               ...
;             ]
; orderClause: "#(compare [(:col1 %1) (:col2 %2)]
;                         [(:col1 %2) (:col2 %1)])"
; ]
(defn parseQuery
  [query_raw_raw commands_list]
  (println "==================PARSEQUERY==================")
  (def query_raw (processQuery query_raw_raw))
  (print "query_raw: ")
  (println query_raw)
  (def commands (getCommands query_raw commands_list))
  (print "commands: ")
  (println commands)
  (print "The amount of arguments in commands: ")
  (println (count commands))
  (def file (cond
              (not= -1 (.indexOf commands "where"))
              (subvec query_raw
                      (+ 1 (.indexOf query_raw "from"))
                      (.indexOf query_raw "where"))
              (and (not= -1 (.indexOf commands "order by")) (= -1 (.indexOf commands "where")))
              (subvec query_raw
                      (+ 1 (.indexOf query_raw "from"))
                      (.indexOf query_raw "order by"))
              (= -1 (.indexOf commands "where")) (subvec query_raw (+ 1 (.indexOf query_raw "from")))))
  (print "file: ")
  (println file)
  (def columns (getColumns query_raw commands file))
  (print "columns: ")
  (println columns)
  (def query (apply conj file columns))
  (print "query: ")
  (println query)
  (def clause (cond
                (and (not= -1 (.indexOf commands "where"))
                     (= -1 (.indexOf commands "order by"))
                     ; other conditions like 'group by' and so on
                     )
                (getClause
                  (subvec query_raw (+ 1 (.indexOf query_raw "where")))
                  columns)
                (and (not= -1 (.indexOf commands "where"))
                     (not= -1 (.indexOf commands "order by")))
                (getClause
                  (subvec query_raw (+ 1 (.indexOf query_raw "where")) (.indexOf query_raw "order by"))
                  columns)
                (= -1 (.indexOf commands "where")) nil
                ; other conditions
                :else nil
                ))
  (println "================")
  (print "clause: ")
  (println clause)
  (def orderClause (cond
                     (not= -1 (.indexOf commands "order by"))
                     (getOrderClause (subvec query_raw (+ 1 (.indexOf query_raw "order by"))))
                     :else nil))
  (print "orderClause: ")
  (println orderClause)
  (vector commands query clause orderClause))


(defn -main [& args]
  (println "Write your commands here!")
  (print "~> ")
  (flush)
  (def commands_list ["select" "from" "where" "distinct" "order by" "group by"])
  (loop [input (read-line)]
    (executeQuery (parseQuery (clojure.string/split (clojure.string/lower-case (clojure.string/replace input #"[,;]" "")) #" ") commands_list)))
  (recur (-main))
  )

(defn inner-join
  [field1 field2 table1 table2]
  (remove nil? (let [table (if (> (count table1) (count table2))
                             table1
                             table2)
                     other_table (if (<= (count table1) (count table2))
                                   table1
                                   table2)
                     table_count (count table)]
                 (for [i (range 0 table_count)]
                   (let [elem1 (nth table i)
                         elem2 (remove nil? (for [elem other_table]
                                              (if (= (get elem1 (keyword field1)) (get elem (keyword field2)))
                                                elem
                                                nil)))]
                     (if (not= 0 (count elem2))
                       (merge (nth table i) (first elem2))
                       nil)))
                 )))

(def test1
  [{:id 2
    :name "kek"}
   {:id 1
    :name "lol"}
   {:id 3
    :name "rofl"}
   {:id 47
    :name "oi"}])

(def test2
  [{:id 1
    :surname "loli4"}
   {:id 2
    :surname "keki4"}
   {:id 3
    :surname "rofli4"}])