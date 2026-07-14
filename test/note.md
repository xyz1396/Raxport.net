### install dependencies on Linux

Linux release builds use a conda-forge compiler toolchain with
`sysroot_linux-64=2.17`. This keeps the native HDF5 bridge and SQLite runtime
compatible with glibc 2.17 even when building on a newer distribution such as
Ubuntu 24.04.

For a one-command incremental build, run:

```bash
./test/build-linux.sh
```

The script creates or updates the environment, removes `bin/`, reuses installed
vcpkg HDF5 and SQLite libraries when all required artifacts exist, publishes
the single-file executable, checks glibc compatibility, and runs the tests.
To discard and rebuild the native libraries, run:

```bash
RAXPORT_FORCE_VCPKG_REBUILD=1 ./test/build-linux.sh
```

```bash
# Create the micromamba environment used by local builds and Linux CI.
micromamba create -n raxport-sysroot217 -c conda-forge -y \
  sysroot_linux-64=2.17 \
  gcc_linux-64 gxx_linux-64 binutils_linux-64 patchelf \
  dotnet-sdk=8 \
  cmake=3.31 ninja pkg-config make autoconf automake libtool \
  git curl zip unzip tar \
  python=3.12 h5py

micromamba activate raxport-sysroot217

# Raxport's MSBuild native target invokes gcc by name. Conda exposes its
# sysroot-aware compiler through $CC, so provide that name inside the env.
ln -sf "$(command -v "$CC")" "$CONDA_PREFIX/bin/gcc"

# Confirm that the environment and compiler use the requested sysroot.
test "$(micromamba list | awk '$1 == "sysroot_linux-64" { print $2 }')" = 2.17
test -n "${CONDA_BUILD_SYSROOT:-}"
gcc --print-sysroot
```

Use a fresh vcpkg checkout so no libraries built by the host compiler are
reused. vcpkg must receive the conda compiler variables when calculating and
building each package.

```bash
git clone --depth 1 https://github.com/microsoft/vcpkg.git vcpkg
./vcpkg/bootstrap-vcpkg.sh

export VCPKG_KEEP_ENV_VARS='CC;CXX;CONDA_BUILD_SYSROOT'
./vcpkg/vcpkg install hdf5:x64-linux --binarysource=clear
./vcpkg/vcpkg install sqlite3:x64-linux-dynamic --binarysource=clear
```

### compile and test on Linux

```bash
# The linux-x64 publish profile builds a self-contained single-file binary and
# bundles the native HDF5 bridge into the output file.
# Output: bin/Raxport-linux-x64
# Avoid passing conda's RPATH through LDFLAGS; the native build strips the
# additional RPATH injected by conda GCC's specs before bundling the bridge.
unset LDFLAGS
dotnet publish src/RaxportNetCore.csproj \
  /p:PublishProfile=linux-x64-single-file \
  -p:UseSharedCompilation=false

./bin/Raxport-linux-x64 -h

# The HDF5 tests load the freshly built native bridge, and two schema tests use
# the environment's Python and h5py packages.
export LD_LIBRARY_PATH="$PWD/bin/obj/native/linux-x64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
dotnet test src/RaxportNetCore.csproj \
  -p:EnableRaxportTests=true \
  -p:UseSharedCompilation=false \
  --logger 'console;verbosity=minimal'
```

Check every native component that is bundled into the Linux artifact. The
reported maximum GLIBC version must not be newer than 2.17.

```bash
bridge=bin/obj/native/linux-x64/libraxport_hdf5.so
sqlite=vcpkg/installed/x64-linux-dynamic/lib/libsqlite3.so
app=bin/Raxport-linux-x64

for file in "$bridge" "$sqlite" "$app"; do
  printf '%s: ' "$file"
  readelf --version-info "$file" \
    | grep -o 'GLIBC_[0-9.]*' \
    | sort -Vu \
    | tail -n 1
done

# HDF5, zlib, szip, and libaec must be statically linked into the bridge.
! readelf -d "$bridge" | grep -Eq 'Shared library: \[lib(hdf5|z|sz|aec)\.so'
```

AOT publish is not used because it gives Thermo DLL/native interop errors:

```bash
# dotnet publish src/RaxportNetCore.csproj -c Release -r linux-x64 -p:PublishAot=true --self-contained true
```

### set heap size

Linux/macOS bash:

```bash
export DOTNET_GCHeapHardLimit=0x1000000000
```

Windows PowerShell:

```powershell
$env:DOTNET_GCHeapHardLimit = "0x1000000000"
```

Windows CMD:

```cmd
set DOTNET_GCHeapHardLimit=0x1000000000
```

Heap hex values:

```text
0x100000000 = 4 GiB
0x200000000 = 8 GiB
0x400000000 = 16 GiB
0x800000000 = 32 GiB
0x1000000000 = 64 GiB
0x2000000000 = 128 GiB
```

### HDF5 compression level

Raxport defaults to `--hdf5-compression-level 1` to reduce conversion time while keeping gzip compression enabled. Use `--hdf5-compression-level 6` for the older smaller-output behavior, or `--hdf5-compression-level 0` when write speed matters more than file size.

### heap size and -p

`DOTNET_GCHeapHardLimit` is the managed heap limit for each Raxport process. The `-p` option controls how many peak rows are buffered before HDF5 flush: one `-p` unit is 10,000,000 peak rows, so the default `-p 2` buffers about 20,000,000 peaks.

Use a smaller `-p` when the heap limit is small or when running many files with `-j`. Use a larger heap limit before increasing `-p`.

```bash
# Lower memory per process, more frequent HDF5 flushes.
./bin/Raxport-linux-x64 -i 'input path' -o 'output path' -j 6 -p 1

# Default balance, about 20M buffered peaks per process.
export DOTNET_GCHeapHardLimit=0x1000000000
./bin/Raxport-linux-x64 -i 'input path' -o 'output path' -j 6 -p 2

# Higher memory per process, fewer HDF5 flushes.
export DOTNET_GCHeapHardLimit=0x2000000000
./bin/Raxport-linux-x64 -i 'input path' -o 'output path' -j 6 -p 3
```
