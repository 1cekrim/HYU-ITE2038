make
cd builds
cp main /mnt/tmpfs/main
cd /mnt/tmpfs
./main
rm /mnt/tmpfs/*.db
rm /mnt/tmpfs/main
cd ~/github/2020_ite2038_2019027192/project2
make clean