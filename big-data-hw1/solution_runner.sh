#!/bin/sh

if [ $1 = "2_a" ]; then
    python3 ./src/solution_2_a.py -r hadoop ./input/mat1.csv > ./output/result_2_a.csv
elif [ $1 = "2_b" ]; then
    python3 ./src/solution_2_b.py -r hadoop ./input/mat1.csv > ./output/result_2_b.csv
elif [ $1 = "2_c" ]; then
    python3 ./src/solution_2_c.py -r hadoop ./input/mat1.csv > ./output/result_2_c.csv
elif [ $1 = "2_d" ]; then
    python3 ./src/solution_2_d.py -r hadoop ./input/mat1.csv ./input/mat2.csv > ./output/result_2_d.csv
else
    echo "Invalid Solution"
fi
