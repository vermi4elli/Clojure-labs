(ns lab4.core
  (:gen-class)
  (:use [clojure.data.csv]
        [clojure.data.json]))

(require '[clojure.data.csv :as csv]
         '[clojure.java.io :as io]
         '[clojure.data.json :as js])


(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

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
  (vec (with-open [reader (io/reader path)]
         (doall
           (js/read reader)))))

; Using zipmap to create vector of maps with header line as keys
; for data in the following lines
(defn mapData
  [head & lines]
  (vec (map #(zipmap (map keyword head) %1) lines)))

;general function for parsing .csv, .tsv, .json files
(defn loadFile [path]
  (case (subs path (clojure.string/last-index-of path "."))
    ".csv"  (apply mapData (readCSV path))
    ".tsv"  (apply mapData (readTSV path))
    ".json" (readJSON path)
    "Incorrect file path or type!"))

; ========================================
; The interface of 'SELECT' query

