# __proto__

We are given a NxN square matrix in a file, whose elements represent a position on a minesweeper game board. An element can either be "O", meaning that it's empty, or "X", meaning that there is a mine at that position. Devise a function that, given the name of the file containing the matrix, returns a matrix in which every empty cell is replaced by the number of mines in the Moore neighborhood of that cell. The Moore neighborhood comprises the eight cells surrounding the cell, four directly next to it and four diagonal to it. 

The input consists of n lines (each representing a row) and n columns in every line, as shown below. The input matrix must be loaded from a file.

For example, download the minesweeper.txt file from here: http://goo.gl/ql3W7P

INPUT:
X O O X X X O O
O O O O X O X X
X X O X X O O O
O X O O O X X X
O O X X X X O X
X O X X X O X O
O O O X O X O X
X O X X O X O X

OUTPUT:
X 1 1 X X X 3 2
3 3 3 5 X 5 X X
X X 3 X X 5 5 4
3 X 5 5 6 X X X
2 4 X X X X 6 X
X 3 X X X 5 X 3
2 4 5 X 6 X 5 X
X 2 X X 4 X 4 X
