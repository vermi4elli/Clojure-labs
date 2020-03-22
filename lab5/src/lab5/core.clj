(ns lab5.core
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

(defn select
  [query]
  (let [[file & columns] query]
    (for [element (choose_file file)]
      (for [column (vec columns)] (get element (keyword column))))))

; ========================================
; Implementation for SELECT DISTINCT query

(defn select_distinct
  [query]
  (vec (set (select query))))

; ========================================
; Implementation for WHERE query

(defn isdigit?
  [character]
  (if (nil? character)
    false
    (if (and (>= (int character) 48) (<= (int character) 57))
      true
      false)))

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
  (def clauseWord (cond
                (not= -1 (.indexOf clause_undone "and")) "and"
                (not= -1 (.indexOf clause_undone "or")) "or"
                :else nil
                ))
  (def clause (if (not= nil clauseWord)
                (vec (rest clause_undone))
                clause_undone))
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
  ;(println "==================CHECKWHERE==================")
  ;(print "commands: ")
  ;(println commands)
  ;(print "query: ")
  ;(println query)
  ;(print "clause: ")
  ;(println clause)
  (if (not= -1 (.indexOf commands "where"))
    (where select_result clause)
    select_result))

(defn checkSelect
  [query commands]
  ;(println "==================CHECKSELECT==================")
  ;(print "commands: ")
  ;(println commands)
  ;(print "query: ")
  ;(println query)
  (if (not= -1 (.indexOf commands "distinct"))
    (select_distinct query)
    (select query)))

(defn printResult
  [query]
  (clojure.pprint/print-table query))

; turns the stringed numbers into normal numbers, then
; makes a map out of the result data to feed it into order by and, then, printResult
(defn prepareData
  [query columns]
  ;(print "query: ")
  ;(println query)
  ;(print "types of data in query: ")
  ;(println (for [element (first query)]
  ;           (type element)))
  (def query_remade (vec (for [line query]
                           (vec (for [element line]
                                  (if (isdigit? (first element))
                                    (read-string element)
                                    element))))))
  ;(print "query_remade: ")
  ;(println query_remade)
  ;(print "types of data in query_remade: ")
  ;(println (for [element (first query_remade)]
  ;           (type element)))
  (apply mapData (apply vector columns query_remade))
  )

; starts the execution of the correct function
(defn executeQuery
  [parsed_query]
  ;(println "==================EXECUTEQUERY==================")
  (let [commands (nth parsed_query 0)
        query (nth parsed_query 1)
        clause (if (nth parsed_query 2)
                 (nth parsed_query 2)
                 nil)
        orderClause (if (nth parsed_query 3)
                      (nth parsed_query 3)
                      nil)
        columns (vec (rest query))]
    ;(print "commands: ")
    ;(println commands)
    ;(print "query: ")
    ;(println query)
    ;(print "clause: ")
    ;(println clause)
    ;(print "columns: ")
    ;(println columns)
    (printResult (orderBy (prepareData (checkWhere (checkSelect query commands) commands clause) columns) orderClause))
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

; parses the simple clause (e.g.: from "mp_id>=21000" to [ "mp_id" ">=" "21000" ]
;                          from "not mp_id>=21000" to [ "mp_id" "<=" "21000" ])
(defn getSimpleClause
  [clause_undone columns]
  ;(println "==================getSimpleClause==================")
  ;(print "clause_undone: ")
  ;(println clause_undone)
  (def clause (if (not= -1 (.indexOf clause_undone "not"))
                (clojure.string/join " " (subvec clause_undone 1))
                (clojure.string/join " " (subvec clause_undone 0))))
  ;(print "clause: ")
  ;(println clause)
  ;
  (def oppositeOperations {">=" "<=", "<>" "=", "=" "<>", "<=" ">="})
  (def operationsTranslations {">=" ">=", "<=" "<=", "<>" "not=", "=" "="})
  ;
  (def operation (first (remove nil? (for [element (keys operationsTranslations)]
                   (if-not (nil? (clojure.string/index-of clause element)) element)))))
  ;(print "operation: ")
  ;(println operation)
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
  ;(println "==================GETCLAUSE==================")
  ;(print "clause undone: ")
  ;(println clause_undone)
  ;(print "columns: ")
  ;(println columns)
  ;
  (def clauseWord (cond
                    (not= -1 (.indexOf clause_undone "and"))
                      "and"
                    (not= -1 (.indexOf clause_undone "or"))
                      "or"
                    :else nil))
  ;
  ;(println "==================")
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

; gets the vector of all 'columns' from the file we're parsing
(defn getColumnsFromStar
  [file]
  ;(println "==================GETCOLUMNSFROMSTAR==================")
  ;(print "file: ")
  ;(println file)
  (loop [x 0
         result []]
    (if (< x (count (keys (first (choose_file (first file))))))
      (recur (+ x 1)
             (conj result (name (nth (keys (first (choose_file (first file)))) x))))
      result)))

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

; parses the columns and functions
(defn getColumns
  [query_raw commands file]
  (def query (cond ))
  (cond
    (not= (clojure.string/lower-case (nth query_raw 1)) "distinct")
    (if (= (clojure.string/lower-case (nth query_raw 1)) "*")
      (getColumnsFromStar file)
      (subvec query_raw 1 (.indexOf query_raw "from")))
    (= (clojure.string/lower-case (nth query_raw 1)) "distinct")
    (if (= (clojure.string/lower-case (nth query_raw 2)) "*")
      (getColumnsFromStar file)
      (subvec query_raw 2 (.indexOf query_raw "from")))))

; checks if the input equals 'exit', if so exits with the code 0
; else, it changes the separate commands 'order' 'by' to 'order by'
(defn processQuery
  [query_raw_raw]
  (if (= "exit" (first query_raw_raw))
    (System/exit 0)
    (if (not= -1 (.indexOf query_raw_raw "order"))
      (let [raw (assoc query_raw_raw (.indexOf query_raw_raw "order") "order by")]
        (apply conj (subvec raw 0 (.indexOf raw "by")) (subvec raw (+ 1 (.indexOf raw "by")))))
      query_raw_raw)))

; parses the query in the format:
; [ commands: ["command1 (e.g. select)" "command2 (e.g. from)" ...]
;   query:    ["file_name" "column1" "column2" ...]
;   clause:   [
;               here we put either "and" or "or" or nothing if there is only one condition
;               ["index_of_column" ">=/not=" "bound"]
;               ...
;             ]
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
  (print "clause: ")
  (println clause)
  (def query (apply conj file columns))
  (print "query: ")
  (println query)
  (def orderClause (cond
                     (not= -1 (.indexOf commands "order by"))
                        (getOrderClause (subvec query_raw (+ 1 (.indexOf query_raw "order by"))))
                     :else nil))
  (print "orderClause: ")
  (println orderClause)
  (vector commands query clause orderClause))

; ========================================
; Implementation of the ORDER BY fuction

; the sorting algorithm we'll be using to sort everything
(defn quicksort [coll]
  (if (seq coll)
    ; choosing the pivot randomly from the vector
    (let [pivot   (rand-nth coll)
          pivots  (filter #(= 0 (compare % pivot)) coll)
          less    (filter #(> 0 (compare % pivot)) coll)
          greater (filter #(< 0 (compare % pivot)) coll)]
      (apply vector (concat (quicksort less) pivots (quicksort greater))))
    ; return the coll if it's empty
    coll))

(defn -main [& args]
  (println "Write your commands here!")
  (print "~> ")
  (flush)
  (def commands_list ["select" "from" "where" "distinct" "order by"])
  (loop [input (read-line)]
    (executeQuery (parseQuery (clojure.string/split (clojure.string/lower-case (clojure.string/replace input #"[,;]" "")) #" ") commands_list)))
  (recur (-main))
  )