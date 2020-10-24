make clean
make test
rm /mnt/tmpfs/*.db
rm /mnt/tmpfs/test
cd builds
cp test /mnt/tmpfs/test
cd /mnt/tmpfs
time ./test
rm /mnt/tmpfs/*.db
rm /mnt/tmpfs/test
cd ~/github/2020_ite2038_2019027192/project2
make clean