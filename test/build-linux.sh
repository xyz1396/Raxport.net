#!/usr/bin/env bash

set -euo pipefail

readonly environment_name=raxport-sysroot217
readonly target_glibc=2.17
readonly script_path="$(readlink -f "${BASH_SOURCE[0]}")"
readonly repo_root="$(cd -- "$(dirname -- "$script_path")/.." && pwd -P)"
readonly force_vcpkg_rebuild="${RAXPORT_FORCE_VCPKG_REBUILD:-0}"

environment_specs=(
  sysroot_linux-64=2.17
  gcc_linux-64
  gxx_linux-64
  binutils_linux-64
  patchelf
  dotnet-sdk=8
  cmake=3.31
  ninja
  pkg-config
  make
  autoconf
  automake
  libtool
  git
  curl
  zip
  unzip
  tar
  python=3.12
  h5py
)

step() {
  printf '\n==> %s\n' "$*"
}

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: ./test/build-linux.sh

Create or update the glibc 2.17 micromamba environment, reuse installed vcpkg
libraries when available, then build, verify, and test the Linux single-file
executable. Set RAXPORT_FORCE_VCPKG_REBUILD=1 to rebuild native dependencies.
EOF
}

if [[ ! -f "$repo_root/src/RaxportNetCore.csproj" || ! -d "$repo_root/.git" ]]; then
  fail "could not identify the Raxport.net repository root"
fi

case "${1:-}" in
  "") ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    fail "unknown argument: $1"
    ;;
esac

case "$force_vcpkg_rebuild" in
  0 | 1) ;;
  *) fail "RAXPORT_FORCE_VCPKG_REBUILD must be 0 or 1" ;;
esac

if [[ "${RAXPORT_BUILD_IN_ENV:-0}" != 1 ]]; then
  command -v micromamba >/dev/null 2>&1 || fail "micromamba is required but was not found in PATH"

  if micromamba run -n "$environment_name" true >/dev/null 2>&1; then
    step "Updating micromamba environment $environment_name"
    micromamba install -y -n "$environment_name" -c conda-forge \
      --strict-channel-priority "${environment_specs[@]}"
  else
    step "Creating micromamba environment $environment_name"
    micromamba create -y -n "$environment_name" -c conda-forge \
      --strict-channel-priority "${environment_specs[@]}"
  fi

  exec micromamba run -n "$environment_name" \
    env RAXPORT_BUILD_IN_ENV=1 bash "$script_path"
fi

cd -- "$repo_root"

test "$(micromamba list | awk '$1 == "sysroot_linux-64" { print $2 }')" = "$target_glibc"
test -n "${CONDA_PREFIX:-}"
test -n "${CONDA_BUILD_SYSROOT:-}"
test -n "${CC:-}"
test -n "${CXX:-}"

# The MSBuild native target invokes gcc by name. Point it at conda's
# sysroot-aware compiler without changing the project file.
ln -sf "$(command -v "$CC")" "$CONDA_PREFIX/bin/gcc"
test "$(readlink -f "$(command -v gcc)")" = "$(readlink -f "$(command -v "$CC")")"

if [[ -e "$repo_root/vcpkg" && ! -d "$repo_root/vcpkg/.git" ]]; then
  fail "$repo_root/vcpkg exists but is not a vcpkg Git checkout"
fi

step "Cleaning generated .NET build directories"
rm -rf -- "$repo_root/bin"

if [[ "$force_vcpkg_rebuild" == 1 ]]; then
  step "Removing installed vcpkg libraries for a forced rebuild"
  rm -rf -- \
    "$repo_root/vcpkg/buildtrees" \
    "$repo_root/vcpkg/installed" \
    "$repo_root/vcpkg/packages"
fi

if [[ -d "$repo_root/vcpkg/.git" ]]; then
  step "Reusing existing vcpkg checkout and download cache"
else
  step "Cloning vcpkg"
  git clone --depth 1 https://github.com/microsoft/vcpkg.git vcpkg
fi

if [[ ! -x "$repo_root/vcpkg/vcpkg" ]]; then
  step "Bootstrapping vcpkg"
  ./vcpkg/bootstrap-vcpkg.sh -disableMetrics
fi

hdf5_artifacts=(
  vcpkg/installed/x64-linux/include/hdf5.h
  vcpkg/installed/x64-linux/lib/libhdf5.a
  vcpkg/installed/x64-linux/lib/libz.a
  vcpkg/installed/x64-linux/lib/libsz.a
  vcpkg/installed/x64-linux/lib/libaec.a
)
sqlite_artifacts=(
  vcpkg/installed/x64-linux-dynamic/lib/libsqlite3.so
)

artifacts_exist() {
  local artifact

  for artifact in "$@"; do
    [[ -f "$artifact" ]] || return 1
  done
}

if artifacts_exist "${hdf5_artifacts[@]}" && artifacts_exist "${sqlite_artifacts[@]}"; then
  step "Reusing installed HDF5 and SQLite libraries"
else
  step "Installing missing HDF5 and SQLite libraries against glibc $target_glibc"
  export VCPKG_KEEP_ENV_VARS='CC;CXX;CONDA_BUILD_SYSROOT'
  artifacts_exist "${hdf5_artifacts[@]}" || \
    ./vcpkg/vcpkg install hdf5:x64-linux --binarysource=clear
  artifacts_exist "${sqlite_artifacts[@]}" || \
    ./vcpkg/vcpkg install sqlite3:x64-linux-dynamic --binarysource=clear
fi

step "Publishing the Linux single-file executable"
# Avoid passing conda's environment-specific RPATH a second time through
# LDFLAGS. The project also strips the RPATH injected by conda GCC's specs.
unset LDFLAGS
dotnet publish src/RaxportNetCore.csproj \
  /p:PublishProfile=linux-x64-single-file \
  -p:UseSharedCompilation=false

bridge=bin/obj/native/linux-x64/libraxport_hdf5.so
sqlite=vcpkg/installed/x64-linux-dynamic/lib/libsqlite3.so
app=bin/Raxport-linux-x64
readelf_command="${READELF:-readelf}"

command -v "$readelf_command" >/dev/null 2>&1 || fail "readelf was not found"
[[ -f "$bridge" ]] || fail "missing native HDF5 bridge: $bridge"
[[ -f "$sqlite" ]] || fail "missing SQLite runtime: $sqlite"
[[ -x "$app" ]] || fail "missing Linux executable: $app"
[[ ! -e bin/libraxport_hdf5.so ]] || fail "native bridge was not bundled into the executable"

if "$readelf_command" -d "$bridge" | grep -Eq '\((RPATH|RUNPATH)\)'; then
  fail "the HDF5 bridge contains an RPATH or RUNPATH"
fi

if "$readelf_command" -d "$bridge" | grep -Eq 'Shared library: \[lib(hdf5|z|sz|aec)\.so'; then
  fail "the HDF5 bridge has an unexpected dynamic native dependency"
fi

check_glibc() {
  local file="$1"
  local maximum

  maximum="$(
    "$readelf_command" --version-info "$file" \
      | grep -o 'GLIBC_[0-9.]*' \
      | cut -d_ -f2 \
      | sort -Vu \
      | tail -n 1
  )"

  [[ -n "$maximum" ]] || fail "could not determine GLIBC requirements for $file"
  if [[ "$(printf '%s\n%s\n' "$target_glibc" "$maximum" | sort -Vu | tail -n 1)" != "$target_glibc" ]]; then
    fail "$file requires GLIBC_$maximum, newer than GLIBC_$target_glibc"
  fi

  printf '    %s: GLIBC_%s\n' "$file" "$maximum"
}

step "Verifying native compatibility"
check_glibc "$bridge"
check_glibc "$sqlite"
check_glibc "$app"

step "Running tests"
export LD_LIBRARY_PATH="$repo_root/bin/obj/native/linux-x64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
dotnet test src/RaxportNetCore.csproj \
  -p:EnableRaxportTests=true \
  -p:UseSharedCompilation=false \
  --logger 'console;verbosity=minimal'

step "Running CLI smoke test"
"$app" -h >/dev/null

printf '\nBuild complete: %s\n' "$repo_root/$app"
