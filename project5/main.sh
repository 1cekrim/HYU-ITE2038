# make clean
make
rm /mnt/tmpfs/*.db
rm /mnt/tmpfs/main
cd builds
cp main /mnt/tmpfs/main
cd /mnt/tmpfs
# valgrind ./main slock_test >> result.txt
./main slock_test
# cp result.txt ~/github/2020_ite2038_2019027192/project5/result.txt
rm /mnt/tmpfs/*.db
rm /mnt/tmpfs/main
cd ~/github/2020_ite2038_2019027192/project5
make clean