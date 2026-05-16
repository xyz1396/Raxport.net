### install dependencies on linux

```bash
# Create/update the micromamba environment used by this repo.
micromamba create -n sipros5 -c conda-forge dotnet-sdk=8 cmake ninja pkg-config gcc gxx git curl zip unzip tar -y
micromamba activate sipros5

# Install the static HDF5 dependency used by src/native/raxport_hdf5.c.
git clone https://github.com/microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
./vcpkg/vcpkg install hdf5:x64-linux
```

### compile on linux

```bash
# The linux-x64 publish profile builds a self-contained single-file binary and
# bundles the native HDF5 bridge into the output file.
# Output: bin/Raxport-linux-x64
micromamba run -n sipros5 dotnet restore src/RaxportNetCore.csproj
micromamba run -n sipros5 dotnet publish src/RaxportNetCore.csproj /p:PublishProfile=linux-x64-single-file -p:UseSharedCompilation=false
./bin/Raxport-linux-x64 -h
```

AOT publish is not used because it gives Thermo DLL/native interop errors:

```bash
# micromamba run -n sipros5 dotnet publish src/RaxportNetCore.csproj -c Release -r linux-x64 -p:PublishAot=true --self-contained true
```

### set heap size

Linux/macOS bash:

```bash
export DOTNET_GCHeapHardLimit=0x200000000
```

Windows PowerShell:

```powershell
$env:DOTNET_GCHeapHardLimit = "0x200000000"
```

Windows CMD:

```cmd
set DOTNET_GCHeapHardLimit=0x200000000
```

Heap hex values:

```text
0x100000000 = 4 GiB
0x200000000 = 8 GiB
0x400000000 = 16 GiB
```

### heap size and -p

`DOTNET_GCHeapHardLimit` is the managed heap limit for each Raxport process. The `-p` option controls how many peak rows are buffered before HDF5 flush: one `-p` unit is 10,000,000 peak rows, so the default `-p 2` buffers about 20,000,000 peaks.

Use a smaller `-p` when the heap limit is small or when running many files with `-j`. Use a larger heap limit before increasing `-p`.

```bash
# Lower memory per process, more frequent HDF5 flushes.
./bin/Raxport-linux-x64 -i 'input path' -o 'output path' -j 6 -p 1

# Default balance, about 20M buffered peaks per process.
export DOTNET_GCHeapHardLimit=0x200000000
./bin/Raxport-linux-x64 -i 'input path' -o 'output path' -j 6 -p 2

# Higher memory per process, fewer HDF5 flushes.
export DOTNET_GCHeapHardLimit=0x400000000
./bin/Raxport-linux-x64 -i 'input path' -o 'output path' -j 6 -p 3
```
