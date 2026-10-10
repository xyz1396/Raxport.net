#!/usr/bin/env bash
# Create the build environment yourself once, then run ./make.sh.
# The script automatically runs inside the matching micromamba environment.
# Final executable: bin/. Intermediate files and test results: build/.
# Run ./make.sh clean to remove bin/ and build/ without a build environment.
# Managed packages restore through nupkg/NuGet.config: local ThermoFisher
# packages (version pinned in the project), with other dependencies from nuget.org.
# Linux x64 (restrict conda packages to the release's glibc 2.17 baseline):
#   CONDA_OVERRIDE_GLIBC=2.17 micromamba create -y -n raxport -c conda-forge --platform linux-64 --strict-channel-priority \
#     sysroot_linux-64=2.17 gcc_linux-64 binutils_linux-64 patchelf \
#     dotnet-sdk=8 python=3.12 h5py numpy matplotlib pyteomics lxml 'hdf5=*=nompi_*' libsqlite zlib libaec
# macOS Intel (requires Xcode Command Line Tools: xcode-select --install):
#   micromamba create -y -n raxport -c conda-forge --platform osx-64 --strict-channel-priority \
#     dotnet-sdk=8 python=3.12 h5py numpy matplotlib pyteomics lxml 'hdf5=*=nompi_*' libsqlite zlib libaec
# macOS Apple Silicon (requires Xcode Command Line Tools):
#   micromamba create -y -n raxport -c conda-forge --platform osx-arm64 --strict-channel-priority \
#     dotnet-sdk=8 python=3.12 h5py numpy matplotlib pyteomics lxml 'hdf5=*=nompi_*' libsqlite zlib libaec
# Python packages also support test/visualize_raxport_hdf5.py, including mzML input.
# HDF5, SQLite, and their transitive shared libraries are copied from this
# environment, relocated, and bundled into the single-file executable.
#
# Runtime tuning (optional; these settings are not applied during the build):
#   export DOTNET_GCHeapHardLimit=0x1000000000
# Heap limits per process: 0x100000000=4 GiB, 0x200000000=8 GiB,
# 0x400000000=16 GiB, 0x800000000=32 GiB, 0x1000000000=64 GiB,
# 0x2000000000=128 GiB.
# One -p unit buffers 10,000,000 peak rows before HDF5 flush; default -p 2
# buffers about 20M rows. Use a smaller -p for small heaps or many files with
# -j; increase the heap limit before increasing -p. Examples (use the matching
# Raxport-osx-x64 or Raxport-osx-arm64 binary on macOS):
#   ./bin/Raxport-linux-x64 -i 'input path' -o 'output path' -j 6 -p 1
#   export DOTNET_GCHeapHardLimit=0x1000000000
#   ./bin/Raxport-linux-x64 -i 'input path' -o 'output path' -j 6 -p 2
#   export DOTNET_GCHeapHardLimit=0x2000000000
#   ./bin/Raxport-linux-x64 -i 'input path' -o 'output path' -j 6 -p 3
# --hdf5-compression-level 1 (default) favors speed with gzip enabled;
# use 6 for smaller output or 0 to disable compression for faster writes.
# AOT publish is not used because of Thermo DLL/native interop errors.
set -euo pipefail

readonly repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly target_glibc=2.17
readonly environment_name=raxport

step() { printf '\n==> %s\n' "$*"; }
fail() { printf 'error: %s\n' "$*" >&2; exit 1; }
clean() {
  step "Cleaning generated build and publish directories"
  rm -rf -- "$repo_root/build" "$repo_root/bin"
}
usage() {
  cat <<'EOF'
Usage: ./make.sh [build|clean]

With no argument (or build), publish and test. clean (or --clean) removes
bin/ and build/ without requiring micromamba or compiler tools.

Publish and test a self-contained single-file executable in bin/ using
HDF5, SQLite, and build tools from the matching micromamba environment.
Create the environment once using the comments above; activation is automatic.
Targets the current host: Linux x64 (glibc 2.17), macOS x64, or macOS arm64.
Requires micromamba in PATH; macOS also requires Xcode Command Line Tools
(install with: xcode-select --install).
EOF
}

[[ $# -le 1 ]] || fail "expected build, clean, or --help"
[[ -f "$repo_root/src/RaxportNetCore.csproj" ]] || fail "could not identify the repository root"
case "${1:-}" in
  "" | build) ;;
  clean | --clean) clean; exit 0 ;;
  -h | --help) usage; exit 0 ;;
  *) fail "unknown argument: $1" ;;
esac

case "$(uname -s)/$(uname -m)" in
  Linux/x86_64)
    rid=linux-x64
    native_extension=so
    ;;
  Darwin/x86_64)
    rid=osx-x64
    native_extension=dylib
    ;;
  Darwin/arm64)
    rid=osx-arm64
    native_extension=dylib
    ;;
  *) fail "unsupported host: $(uname -s)/$(uname -m); use make.ps1 on Windows" ;;
esac

if [[ "${RAXPORT_BUILD_IN_ENV:-0}" != 1 ]]; then
  command -v micromamba >/dev/null 2>&1 || fail "micromamba is required but was not found in PATH"
  micromamba run -n "$environment_name" true >/dev/null 2>&1 ||
    fail "cannot use environment $environment_name; create it using the command at the top of make.sh"
  step "Using micromamba environment $environment_name"
  exec micromamba run -n "$environment_name" \
    env RAXPORT_BUILD_IN_ENV=1 bash "$repo_root/make.sh"
fi

if [[ "$rid" == osx-* ]]; then
  xcrun --find clang >/dev/null 2>&1 || fail "install Xcode Command Line Tools with: xcode-select --install"
fi

cd -- "$repo_root"
[[ -n "${CONDA_PREFIX:-}" ]] || fail "micromamba did not activate the build environment"
if [[ "$rid" == linux-x64 ]]; then
  [[ "$(micromamba list -p "$CONDA_PREFIX" | awk '$1 == "sysroot_linux-64" { print $2 }')" == "$target_glibc" ]] ||
    fail "the Linux build requires sysroot_linux-64=$target_glibc"
  [[ -n "${CONDA_BUILD_SYSROOT:-}" && -n "${CC:-}" ]] ||
    fail "the conda compiler and sysroot variables were not activated"
else
  export CC="$(xcrun --find clang)"
fi

step "Checking micromamba native dependencies"
for dependency in "$CONDA_PREFIX/include/hdf5.h" \
  "$CONDA_PREFIX/lib/libhdf5.$native_extension" "$CONDA_PREFIX/lib/libsqlite3.$native_extension"; do
  [[ -f "$dependency" ]] || fail "missing dependency: $dependency; install the packages listed at the top of make.sh"
done
clean
mkdir -p -- "$repo_root/build/tmp"
export TMPDIR="$repo_root/build/tmp"
export TMP="$TMPDIR" TEMP="$TMPDIR"
export DOTNET_BUNDLE_EXTRACT_BASE_DIR="$repo_root/build/bundle"

step "Restoring managed packages from nupkg/ and nuget.org"
dotnet restore src/RaxportNetCore.csproj \
  --configfile "$repo_root/nupkg/NuGet.config" \
  "/p:PublishProfile=$rid-single-file"

step "Publishing the $rid single-file executable"
# The native build helper relocates copies of all shared libraries to $ORIGIN
# (Linux) or @loader_path (macOS) before .NET bundles them.
unset LDFLAGS
dotnet publish src/RaxportNetCore.csproj --no-restore \
  "/p:PublishProfile=$rid-single-file" -p:UseSharedCompilation=false

bridge="build/native/$rid/libraxport_hdf5.$native_extension"
app="bin/Raxport-$rid"
[[ -f "$bridge" ]] || fail "missing native HDF5 bridge: $bridge"
[[ -x "$app" ]] || fail "missing executable: $app"
[[ ! -e "bin/libraxport_hdf5.$native_extension" ]] || fail "native bridge was not bundled into the executable"
[[ ! -e "bin/libsqlite3.$native_extension" ]] || fail "SQLite runtime was not bundled into the executable"

check_glibc() {
  local file="$1" maximum
  maximum="$("$readelf_command" --version-info "$file" | grep -o 'GLIBC_[0-9.]*' | cut -d_ -f2 | sort -Vu | tail -n 1)"
  [[ -n "$maximum" ]] || fail "could not determine GLIBC requirements for $file"
  if [[ "$(printf '%s\n%s\n' "$target_glibc" "$maximum" | sort -Vu | tail -n 1)" != "$target_glibc" ]]; then
    fail "$file requires GLIBC_$maximum, newer than GLIBC_$target_glibc"
  fi
  printf '    %s: GLIBC_%s\n' "$file" "$maximum"
}

step "Verifying native compatibility"
if [[ "$rid" == linux-x64 ]]; then
  readelf_command="${READELF:-readelf}"
  command -v "$readelf_command" >/dev/null 2>&1 || fail "readelf was not found"
  # The native helper already checks every bundled library against this limit.
  check_glibc "$app"
  export LD_LIBRARY_PATH="$repo_root/build/native/$rid${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
else
  export DYLD_LIBRARY_PATH="$repo_root/build/native/$rid${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
fi

step "Running tests"
PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -s test -p test_native_runtime.py
dotnet restore src/RaxportNetCore.csproj \
  --configfile "$repo_root/nupkg/NuGet.config" -p:EnableRaxportTests=true
dotnet test src/RaxportNetCore.csproj --no-restore \
  -p:EnableRaxportTests=true -p:UseSharedCompilation=false \
  --results-directory "$repo_root/build/TestResults" \
  --logger 'console;verbosity=minimal'
step "Running CLI smoke test"
"$app" -h >/dev/null
# bin/ is a distribution directory: any sidecar or build directory is an error.
for published_file in "$repo_root/bin/"* "$repo_root/bin/".[!.]* "$repo_root/bin/"..?*; do
  [[ -e "$published_file" || -L "$published_file" ]] || continue
  [[ "$published_file" == "$repo_root/$app" ]] || fail "unexpected publish output: $published_file"
done
printf '\nBuild complete: %s\n' "$repo_root/$app"
