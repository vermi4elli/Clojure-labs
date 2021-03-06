(ns lab3.core
  (:gen-class)
  (:use [clojure.data.csv]
        [clojure.data.json]))

(require '[clojure.data.csv :as csv]
         '[clojure.java.io :as io]
         '[clojure.data.json :as js])

(defn -main
  [call]
  (if (string? call)
    (cond
      (= "load(" (subs call 0 5)) (load (subs call 6 (- (clojure.string/last-index-of call ")") 1)))
      :else (println "Sorry, can't process this"))
    (println "Sorry, can't process this")))

; ========= FIRST task - parsing the files ==========

;1.1) ----------- parsing the FIRST MANDATORY file 'mp-posts_full'
(def mp-posts_full (vec (rest (vec (with-open [reader (io/reader "../data files/mp-posts_full.csv")]
                                     (doall
                                       (csv/read-csv reader)))))))

;1.2) ----------- parsing the SECOND MANDATORY file 'map_zal-skl9'
(def map_zal-skl9 (vec (rest (vec (with-open [reader (io/reader "../data files/map_zal-skl9.csv")]
                                    ; due to the fact that read-csv produces lazy-seq,
                                    ; we use the 'doall' function that forces all lazy-seq
                                    (doall
                                      (csv/read-csv reader)))))))

;1.3) ----------- parsing the THIRD MANDATORY file 'plenary_register_mps-skl9'
(def plenary_register_mps-skl9_raw (vec (doall
                 (line-seq
                   (io/reader "../data files/plenary_register_mps-skl9.tsv")))))

;writing the final structure into 'plenary_register_mps-skl9_final'
(def plenary_register_mps-skl9_final (vec (for [string plenary_register_mps-skl9_raw]
       (vec (clojure.string/split
              (clojure.string/replace string #"\t" "|") #"\|")))))

;checking the first element of the resulting structure
(nth plenary_register_mps-skl9_final 1)

;2) ----------- parsing the ADDITIONAL file 'plenary_vote_results-skl9'
(def plenary_vote_results-skl9_raw (vec (doall
                 (line-seq
                   (io/reader "../data files/plenary_vote_results-skl9.tsv")))))

;writing the final structure into 'plenary_vote_results-skl9_final'
(def plenary_vote_results-skl9_final (vec (for [string plenary_vote_results-skl9_raw]
  (vec (clojure.string/split
        (clojure.string/replace string #"\t" "|") #"\|")))))

;checking the first element of the resulting structure
(nth plenary_vote_results-skl9_final 1)

;3) ----------- parsing the EXTRA file 'mps-declarations_rada'
(def mps-declarations_rada (vec (with-open [reader (io/reader "../data files/mps-declarations_rada.json")]
                                  (doall
                                    (js/read reader)))))


; ========= SECOND task - writing a general function 'loadFile' for opening all file types ==========
; also writing functions for each file type to make the code simpler

;function for .csv parsing
(defn readCSV [path]
  (vec (rest (vec (with-open [reader (io/reader path)]
                    (doall
                      (csv/read-csv reader)))))))

;function for .tsv parsing
(defn readTSV [path]
  (vec (for [string (vec (doall
                           (line-seq
                             (io/reader path))))]
         (vec (clojure.string/split
                (clojure.string/replace string #"\t" "|") #"\|")))))

;function for .json parsing
(defn readJSON [path]
  (vec (with-open [reader (io/reader path)]
         (doall
           (js/read reader)))))

;general function for parsing .csv, .tsv, .json files
(defn loadFile [path]
  (case (subs path (clojure.string/last-index-of path "."))
    ".csv"  (readCSV path)
    ".tsv"  (readTSV path)
    ".json" (readJSON path)
    "Incorrect file path or type!"))

; ========= THIRD task - writing a general function 'load' for printing all files ==========

(defn printCSV [path]
  (for [line (vec (with-open [reader (io/reader path)]
         (doall
           (csv/read-csv reader))))]
    (println line)))

(defn printTSV [path]
  (for [line (vec (for [string (vec (doall
                                    (line-seq
                                      (io/reader path))))]
                  (vec (clojure.string/split
                         (clojure.string/replace string #"\t" "|") #"\|"))))]
    (println line)))

(defn printJSON [path]
  (for [line (vec (with-open [reader (io/reader path)]
                  (doall
                    (js/read reader))))]
    (println line)))

(defn load [path]
  (case (subs path (clojure.string/last-index-of path "."))
    ".csv"  (printCSV path)
    ".tsv"  (printTSV path)
    ".json" (printJSON path)
    "Incorrect file path or type!"))