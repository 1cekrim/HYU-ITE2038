make test
cd builds
cp test /mnt/tmpfs/test
cd /mnt/tmpfs
./test
gprof test gmon.out >> ~/github/2020_ite2038_2019027192/project2/report.txt
rm /mnt/tmpfs/gmon.out
rm /mnt/tmpfs/*.db
rm /mnt/tmpfs/test
cd ~/github/2020_ite2038_2019027192/project2
make clean