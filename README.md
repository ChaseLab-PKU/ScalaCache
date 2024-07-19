# ScalaCache

*Scalable User-Space Page Cache Management with Software-Hardware Coordination*

Open-source code repo for ScalaCache, USENIX ATC'24.

## Setup

### Clone ScalaCache

```bash
git clone git@github.com:ChaseLab-PKU/ScalaCache.git
cd ScalaCache
# configure SPDK
cd SPDK
git submodule update --init --recursive
cd ..
```

### Installation

```bash
# build FEMU
cd FEMU
mkdir build-femu
# switch to the FEMU building directory
cd build-femu
cp ../femu-scripts/femu-copy-scripts.sh .
./femu-copy-scripts.sh .
# only Debian/Ubuntu based distributions supported
sudo ./pkgdep.sh
# compile & install FEMU
./femu-compile.sh

cd ..

# please copy SPDK to the FEMU VM

# run FEMU VM
# build SPDK (assuming in FEMU VM)
cd SPDK
# prerequisites
./scripts/pkgdep.sh
# build 
./configure
make

# build ScalaCache successfully
```

### Run & Evaluation

We provide a fio-like test tool in `SPDK/example/nvme/perf_mt` called perf_mt, which is customized from perf test tool (in `SPDK/example/nvme/perf`) provided by SPDK. Specifically, this tool supports replaying block traces with multiple threads on top of the SPDK I/O engine, and collects statistics such as bandwidth and latency. Configuration descriptions (e.g., specific block trace formats) for this tool can be found in `perf_mt.c` file.
