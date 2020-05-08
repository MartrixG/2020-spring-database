rm *.tbl
cd src
g++ -std=c++11 -g *.cpp exceptions/*.cpp -I. -Wall -o excute