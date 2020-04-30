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
; Implementation for SELECT query

(defn Min
  [file column]
  (let [file_column (for [line (choose_file file)]
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
  (let [file_column (for [line (choose_file file)]
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
  (let [file_column (for [line (choose_file file)]
                      (get line (keyword column)))]
    (count (remove nil? file_column)))
  )

(defn callFunction
  [column_raw]
  (let [file (get column_raw :file)
        column (get column_raw :column)]
    (case (get column_raw :function)
      "count" (Count file column)
      "sum" (Sum file column)
      "min" (Min file column)
    )))

(defn callFunctions
  [columns]
  (apply merge (for [column columns]
                 (assoc {} (keyword (str (get column :function)
                                         "("
                                         (get column :column)
                                         ")")) (callFunction column)))))

(defn full-outer-join
  [field1 field2 table1 table2])

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

(defn select
  [query commands]
  (println "================SELECT===============")
  (cond
    ;first condition
    (and (some #(= "inner join" %) commands)
         true
         ; other conditions
         )
    ; first action
    (inner-join query "" "" "")
    ; second condition
    (and (some #(= "full outer join" %) commands)
         true
         ; other conditions
         )
    ; second action
    (full-outer-join query "" "" "")
    :else (let [columns (get query :columns)
                file (get query :file)
                columns_usual_vector (vec (remove nil? (for [column columns]
                                                    (if (nil? (get column :function))
                                                      (keyword (get column :column))
                                                      nil))))
                columns_functions_vector (vec (remove nil? (for [column columns]
                                                        (if (some? (get column :function))
                                                          column
                                                          nil))))
                select_functions (callFunctions columns_functions_vector)
                select_usual (for [line (choose_file file)]
                               (select-keys line columns_usual_vector))]
            (if (empty? select_functions)
              select_usual
              (merge (first select_usual) select_functions))
            )))

; ========================================
; Implementation for SELECT DISTINCT query
(defn select_distinct
  [query commands]
  (vec (set (select query commands))))

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
                         data
                         (get clause :bound)
                         ")")))))

; select count(mp_id) from mp-posts_full;
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
  (let [clauseWord (get clause_undone :clauseWord)
        clause (get clause_undone :clause)]
    (remove nil? (for [element selected_data]
                     (when (check_true clause clauseWord element) element)))))

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
  (if (not= -1 (.indexOf commands "where"))
    (where selected_data clause)
    selected_data))

(defn checkSelect
  [query commands]
  (println "==================CHECKSELECT==================")
  (if (not= -1 (.indexOf commands "distinct"))
    (select_distinct query commands)
    (select query commands)))

(defn printResult
  [query]
  (clojure.pprint/print-table query))

; select count(mp_id) from mp-posts_full;
; select count(mp_id), count(full_name) from mp-posts_full;
; select distinct mp_id from mps-declarations_rada;
; select distinct mp_id from mps-declarations_rada;

; turns the stringed numbers into normal numbers, then
; makes a map out of the result data to feed it into order by and, then, printResult

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
    (printResult (orderBy (checkWhere (checkSelect query commands) commands clause) orderClause))
    ))

; select distinct mp_id, full_name from mp-posts_full where mp_id>=21200 or mp_id<=9000;
; select distinct mp_id, full_name from mp-posts_full where mp_id>=21200 or not full_name<>'Яцик Юлія Григорівна';

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

; select distinct mp_id, full_name from mp-posts_full where not mp_id>=21100;
; select distinct mp_id, full_name from mp-posts_full where mp_id>=21900 order by mp_id;
; select distinct mp_id, full_name from mp-posts_full where mp_id>=21100;
; select distinct mp_id, full_name from mp-posts_full where not full_name<>'Яцик Юлія Григорівна';
; select distinct mp_id, full_name from mp-posts_full where not full_name<>'Яцик Юлія Григорівна' or not full_name<>'Яцик Юлія Григорівна';
; select distinct mp_id, full_name from mps-declarations_rada;

; parses the simple clause (e.g.: from "mp_id>=21000" to { :column "mp_id"
;                                                          :operation ">="
;                                                          :bound "21000" }
;                                 from "not mp_id>=21000" to { :column "mp_id"
;;                                                             :operation "<="
;;                                                             :bound "21000" })
(defn getSimpleClause
  [clause_undone columns_raw]
  (println "==================getSimpleClause==================")
  (let [columns (vec (for [element columns_raw]
                       (if (nil? (get element :function))
                         (get element :column)
                         (str (get element :function)
                              "("
                              (get element :column)
                              ")"))))
        clause (if (not= -1 (.indexOf clause_undone "not"))
                 (clojure.string/join " " (subvec clause_undone 1))
                 (clojure.string/join " " (subvec clause_undone 0)))
        oppositeOperations {">=" "<=", "<>" "=", "=" "<>", "<=" ">="}
        operationsTranslations {">=" ">=", "<=" "<=", "<>" "not=", "=" "="}
        operation (first (remove nil? (for [element (keys operationsTranslations)]
                                        (if-not (nil? (clojure.string/index-of clause element)) element))))
        column (str (.indexOf columns (subs clause 0 (clojure.string/index-of clause operation))))
        finalOperation (get operationsTranslations (if (not= -1 (.indexOf clause_undone "not"))
                                                     (get oppositeOperations operation)
                                                     operation))
        bound (str (subs clause (+ (count operation) (clojure.string/index-of clause operation))))]
    {:column column
     :operation finalOperation
     :bound (if (and (starts-with? bound "'") (ends-with? bound "'"))
              (clojure.string/replace bound "'" "\"")
              bound)}))

; parses the clause
(defn getClause
  [clause_undone columns]
  (println "==================GETCLAUSE==================")
  (let [clauseWord (cond
                     (not= -1 (.indexOf clause_undone "and"))
                     "and"
                     (not= -1 (.indexOf clause_undone "or"))
                     "or"
                     :else nil)]
    (cond
      (nil? clauseWord) {:clauseWord nil :clause (vector (getSimpleClause clause_undone columns))}
      (= "and" clauseWord) (:clauseWord "and" :clause (for [element (parseComplexClause clause_undone clauseWord)]
                                                        (getSimpleClause element columns)))
      (= "or" clauseWord) (:clauseWord "or" :clause (for [element (parseComplexClause clause_undone clauseWord)]
                                                      (getSimpleClause element columns)))
      )))

; mp_id name asc
; mp_id asc name desc
(defn getOrderClause
  [query_raw]
  ;(println "==================GETORDERCLAUSE==================")
  (let [query_remade (loop [query []
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
                         query))
        firstVector (join "" (vector
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
                               "]"))
        secondVector (join "" (vector
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
                                "]"))]
    (str
      "#(compare "
      (read-string firstVector)
      (read-string secondVector)
      ")")))

; select distinct * from map_zal-skl9 where row>=12 and row<=13 order by row asc, col desc;
; select distinct row, col from map_zal-skl9 where row>=12 and row<=13 order by row asc, col desc;


; gets the vector of all 'columns' from the file we're parsing
(defn getColumnsFromStar
  [file]
  ;(println "==================GETCOLUMNSFROMSTAR==================")
  (let [columns_raw (keys (first (choose_file file)))]
    (for [elem columns_raw]
      {:column (name elem)
       :function nil
       :file file})))


; select distinct mp_id, full_name from mp-posts_full order by mp_id;

; checks if the functions in columns are known
(defn processColumns
  [columns]
  (let [columnsFunctions ["count" "sum" "min" nil]]
    (for [column columns]
      (let [function (get column :function)]
        (if (nil? (some #(= function %) columnsFunctions))
          (throw (Exception. (str "The function " function " is not known!")))
          column)))))

; select count(mp_id), count(full_name) from mp-posts_full;

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
  (println "==================GETCOLUMNS==================")
  (let [columns (subvec query_raw
                      (if (not= -1 (.indexOf commands "distinct"))
                        2
                        1)
                      (.indexOf query_raw "from"))]
    (processColumns (flatten (for [col columns]
      (let [indexLeft (index-of col "(")
            indexRight (index-of col ")")
            indexDot (index-of col ".")
            column (cond
                     (some? indexLeft) (subs col (+ 1 indexLeft) indexRight)
                     (some? indexDot) (subs col (+ 1 indexDot))
                     :else col)
            file_temp (if (some? indexDot)
                        (subs col 0 indexDot)
                        file)
            function (if (some? indexLeft)
                       (if (some? indexDot)
                         (subs col (+ 1 indexDot) indexLeft)
                         (subs col 0 indexLeft))
                       nil)]
        (if (= "*" column)
          (getColumnsFromStar file_temp)
          {:column column
           :function function
           :file file_temp})
        ))))))

; replaces '_type_' 'join' strings in the query to one '_type_ join' string
(defn checkJoinFunction
  [query]
  (let [indexJoin (.indexOf query "join")
        indexFull (.indexOf query "full")
        indexInner (.indexOf query "inner")]
    (if (not= -1 indexJoin)
      (if (not= -1 indexFull)
        (let [before (subvec query 0 indexFull)
              after (subvec query (+ 1 indexJoin))]
          (apply conj before "full outer join" after))
        (let [before (subvec query 0 indexInner)
              after (subvec query (+ 1 indexJoin))]
          (apply conj before "inner join" after)))
      query)))

; replaces '_command_' 'by' strings in the query to one '_command_ by' string
(defn checkByFunction
  [query word]
  ;(println "================CHECKBYFUNCTION=================")
  (def mainWord (first (split word #" ")))
  (if (not= -1 (.indexOf query mainWord))
    (let [raw (assoc query (.indexOf query mainWord) word)]
      (apply conj (subvec raw 0 (.indexOf raw "by")) (subvec raw (+ 1 (.indexOf raw "by")))))
    query))

; gets those commands from the commands_list in the vector,
; which are present in the query
(defn getCommands
  [query commands_list]
  (filterv (fn [x] (if (not= -1 (.indexOf commands_list (clojure.string/lower-case x)))
                     true
                     false))
           query))

; checks if the input equals 'exit', if so exits with the code 0
; else, it changes the separate commands '_command_' 'by' to '_command_ by'
(defn joinSeparateCommands
  [query_raw_raw]
  (if (= "exit" (first query_raw_raw))
    (System/exit 0)
    (-> (checkByFunction query_raw_raw "order by")
        (checkByFunction "group by")
        (checkJoinFunction))))

; select count(mp_id), count(full_name) from mp-posts_full;

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
  (let [query_raw (joinSeparateCommands query_raw_raw)
        commands (getCommands query_raw commands_list)
        file (nth query_raw (+ 1 (.indexOf query_raw "from")))
        columns (getColumns query_raw commands file)
        query {:file file
               :columns columns}
        clause (if (not= -1 (.indexOf commands "where"))
                 (cond
                   ; first condition
                   (and (= -1 (.indexOf commands "order by"))
                        true
                        ; other conditions like 'group by' and so on
                        )
                   ; first action
                   (getClause
                     (subvec query_raw (+ 1 (.indexOf query_raw "where")))
                     columns)
                   ; second condition
                   (and (not= -1 (.indexOf commands "order by"))
                        true
                        ; other conditions
                        )
                   ; second action
                   (getClause
                     (subvec query_raw (+ 1 (.indexOf query_raw "where")) (.indexOf query_raw "order by"))
                     columns)
                   ; else condition and action
                   :else nil
                   )
                 nil)
        orderClause (cond
                      (not= -1 (.indexOf commands "order by"))
                      (getOrderClause (subvec query_raw (+ 1 (.indexOf query_raw "order by"))))
                      :else nil)]
    (vector commands query clause orderClause)))


(defn -main [& args]
  (println "Write your commands here!")
  (print "~> ")
  (flush)
  (def commands_list ["select" "from" "where" "distinct" "order by" "group by" "inner join" "full outer join" "left join" "on"])
  (loop [input (read-line)]
    (executeQuery (parseQuery (clojure.string/split (clojure.string/lower-case (clojure.string/replace input #"[,;]" "")) #" ") commands_list)))
  (recur (-main)))

(def input_test
  ["select" "*" "from" "mp-posts_full" "inner" "join" "mp" "on" "mp_id"])

(def test1
  [{:id 2
    :name "kek"}
   {:id 2
    :name "lol"}
   {:id 1
    :name "lol"}
   {:id 3
    :name "rofl"}
   {:id 47
    :name "oi"}])

(def test0
  [{:id 2}
   {:id 2}
   {:id 1}
   {:id 3}
   {:id 47}])

(def test2
  [{:id 1
    :surname "loli4"}
   {:id 2
    :surname "keki4"}
   {:id 3
    :surname "rofli4"}])

(def test3
  [{:function "123"}])

(def test-print
  [{:id 2, :name "kek" :function 123}
   {:id 2, :name "lol" :function nil}
   {:id 1, :name "lol" :function nil}
   {:id 3, :name "rofl" :function nil}
   {:id 47, :name "oi" :function nil}])

(def query_temp
  {:file "mp-posts_full"
   :columns [
             {:column "mp_id"
              :function nil
              :file "mp-posts_full"}
             {:column "full_name"
              :function nil
              :file "mp-posts_full"}
             ]})