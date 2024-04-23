(ns geometry.noder-test
  (:require [geometry.noder :as noder]
            [geometry.core :as g]
            [clojure.test :refer :all]))

(deftest plain-noding-test
  (let [lines [(g/make-line-string
                [ [-1 0] [1 0] ])

               (g/make-line-string
                [ [0 -1] [0 1] ])]

        noded (noder/node lines)]

    (is (= 4 (count noded)))
    (is (=
         #{(g/make-point -1 0)
           (g/make-point 1 0)
           (g/make-point 0 1)
           (g/make-point 0 -1)
           (g/make-point 0 0)}
         (set (mapcat g/endpoints-of noded))))))

(deftest node-connect-test
  (let [lines [(g/make-line-string [[0 0] [10 0]])

               (g/make-line-string [[0 20] [10 20]])]

        points [(g/make-point 3 5) (g/make-point 3 15)]

        [lines mapping]
        (noder/node-with-others lines points)
        ]
    (is (=
         {(g/make-point 3 5) (g/make-point 3 5)
          (g/make-point 3 15) (g/make-point 3 15)}
         mapping))

    (is (= #{(g/make-line-string [[0 0] [3 0]])
             (g/make-line-string [[3 0] [10 0]])
             (g/make-line-string [[0 20] [3 20]])
             (g/make-line-string [[3 20] [10 20]])
             (g/make-line-string [[3 0] [3 5]])
             (g/make-line-string [[3 20] [3 15]])}
           (set lines)))))

(deftest node-connect-face-test
  (let [lines [(g/make-line-string [[0 0] [10 1]])]
        square (g/make-polygon
                [[4 1] [6 1]
                 [6 3] [4 3]
                 [4 1]])
        [_lines mapping0] (noder/node-with-others lines [square])
        
        [_lines mapping1] (noder/node-with-others lines [square]
                                                :connect
                                                #(noder/connect-faces
                                                  2 1 %1 %2))]
    ;; prefers the middle of the square in second case
    (is (= (g/make-point 6 1) (get mapping0 square)))
    (is (= (g/make-point 5 1) (get mapping1 square)))))

