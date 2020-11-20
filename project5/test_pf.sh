make test
cd builds
rm /mnt/tmpfs/gmon.out
rm /mnt/tmpfs/*.db
rm /mnt/tmpfs/test
cp test /mnt/tmpfs/test
cd /mnt/tmpfs
./test
gprof test >> ~/github/2020_ite2038_2019027192/project3/report.txt
cat ~/github/2020_ite2038_2019027192/project3/report.txt | ~/.local/bin/gprof2dot | dot -Tpng -o output.png
cp output.png ~/github/2020_ite2038_2019027192/project3/result.png
rm /mnt/tmpfs/gmon.out
rm /mnt/tmpfs/*.db
rm /mnt/tmpfs/test
cd ~/github/2020_ite2038_2019027192/project3
make clean