# make clean
make
rm /mnt/tmpfs/*.db
rm /mnt/tmpfs/main
cd builds
cp main /mnt/tmpfs/main
cd /mnt/tmpfs
./main deadlock_test
cp insert.db ~/github/2020_ite2038_2019027192/project2/insert.db
rm /mnt/tmpfs/*.db
rm /mnt/tmpfs/main
cd ~/github/2020_ite2038_2019027192/project2
make clean