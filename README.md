## Dolphin
Dolphin: A Resource-efficient Hybrid Index on Disaggregated Memory

### Build
1. Dependency
```
tar xvf MLNX_OFED_LINUX-4.9-7.1.0.0-ubuntu20.04-x86_64.tgz
./mlnxofedinstall --without-dkms --add-kernel-support --kernel 5.4.0-144-generic --without-fw-update --force --with-rdma --with-nvmf --without-neohost-backend

/etc/init.d/openibd restart
/etc/init.d/opensmd restart

ip link set ibp132s0 down
ip link set ibp132s0 name ib0
ip link set ib0 up

ip addr add 192.xxx.xxx.xxx/xx dev ib0

apt install cmake libboost-all-dev libmemcached-dev libtbb-dev

git clone https://github.com/google/cityhash.git

./configure --prefix=/home/lib/cityhash/
make all check CXXFLAGS="-g -O3"
make install
```
2. CMake
```
./build.sh
```
3. Create HugePage
```
sysctl -w vm.nr_hugepages=32768
```
### Run
```
configure `memcached.conf`, where the 1st line is memcached IP, the 2nd is memcached port

./restartMemc.sh
In each server, execute `./benchmark kNodeCount kReadRatio kThreadCount`
```

### Acknowledgements
Dolphin is optimized based on [Sherman](https://github.com/thustorage/Sherman) and [SMART](https://github.com/dmemsys/SMART), and we would like to thank the authors of Sherman and SMART for releasing their code.