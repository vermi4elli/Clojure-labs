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
          pivots  (filter #(= % pivot) coll)
          less    (filter #(< % pivot) coll)
          greater (filter #(> % pivot) coll)]
      (apply vector (concat (quicksort less) pivots (quicksort greater))))
    ; return the coll if it's empty
    coll))

; Just calling the function
(quicksort [252 32 1  4 23 2])

