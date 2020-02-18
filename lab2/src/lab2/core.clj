(ns lab2.core
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))

; The first task of the lab2
; This function get three arguments: x -> [1]   - vector with an element 1
;                                    y -> 0     - temp counter, will be used
;                                                 to check if we reached the limit
;                                    z -> limit - the amount of elements needed
(defn GenerateSequenceDumbMethod
  [x y z]
  (if (< y (- z 1)) (let [value (* -1 (* 2 (peek x)))
                          counter (+ y 1)]
                          (GenerateSequenceDumbMethod (conj x value) counter z))
                    (println x)))

; Just calling the function
(GenerateSequenceDumbMethod [1] 0 4)

; The second task of the lab2
; quicksort - the sorting algorithm
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

; Just calling the function
(quicksort [252 32 1  4 23 2])

; The third task of the lab3
; written the text
(def text "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt\nmollit anim id est laborum.")
; deleted symbols '.', ','
(def cleanText (clojure.string/split (clojure.string/replace text #"[,.\\]" "") #" "))
; 1) quicksorting the word sequence by the alphabet
(quicksort cleanText)

; 2) quicksorting the word sequence by the amount of letters

; creating a map "amount of letters" -> [words]
(def cleanTextLetterAmount (group-by count cleanText))

