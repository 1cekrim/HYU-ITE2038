make test
cd builds
./test
# gprof test gmon.out >> ~/github/2020_ite2038_2019027192/project2/report.txt
gprof test >> ~/github/2020_ite2038_2019027192/project2/report.txt
cat ~/github/2020_ite2038_2019027192/project2/report.txt | ~/.local/bin/gprof2dot | dot -Tpng -o output.png
cp output.png ~/github/2020_ite2038_2019027192/project2/result.png
make clean