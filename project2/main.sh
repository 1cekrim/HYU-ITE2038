make
cd builds
cp main /mnt/tmpfs/main
cd /mnt/tmpfs
./main
cp sample.db ~/github/2020_ite2038_2019027192/project2/sample.db
rm /mnt/tmpfs/*.db
rm /mnt/tmpfs/main
cd ~/github/2020_ite2038_2019027192/project2
# make clean