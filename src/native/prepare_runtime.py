#!/usr/bin/env python3
"""Build the HDF5 bridge and stage relocatable micromamba runtime libraries."""

import argparse
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import subprocess
import sys


def run(*args):
    return subprocess.check_output([str(arg) for arg in args], text=True).strip()


def linux_dependencies(path):
    return run('patchelf', '--print-needed', path).splitlines()


def mac_dependencies(path):
    identities = run('otool', '-D', path).splitlines()[1:]
    return [line.strip().split(' (compatibility version', 1)[0]
            for line in run('otool', '-L', path).splitlines()[1:]
            if line.strip().split(' (compatibility version', 1)[0] not in identities]


def windows_dependencies(path):
    import pefile
    with pefile.PE(str(path)) as pe:
        return [entry.dll.decode('ascii')
                for table in ('DIRECTORY_ENTRY_IMPORT', 'DIRECTORY_ENTRY_DELAY_IMPORT')
                for entry in getattr(pe, table, [])]


def check_glibc(path, maximum):
    output = run(os.environ.get('READELF', 'readelf'), '--version-info', path)
    versions = [tuple(map(int, version.split('.')))
                for version in re.findall(r'\bGLIBC_([0-9]+(?:\.[0-9]+)+)', output)]
    if versions and max(versions) > tuple(map(int, maximum.split('.'))):
        required = '.'.join(map(str, max(versions)))
        raise RuntimeError(f'{path.name} requires GLIBC_{required}, newer than {maximum}')


def prepare(prefix, output, rid, max_glibc):
    expected = {'linux-x64': ('Linux', 'x86_64'), 'osx-x64': ('Darwin', 'x86_64'),
                'osx-arm64': ('Darwin', 'arm64'), 'win-x64': ('Windows', 'amd64')}
    host = (platform.system(), platform.machine().lower())
    if host != expected[rid]:
        raise RuntimeError(f'{rid} must be built on its matching host, got {host}')
    windows = rid == 'win-x64'
    mac = rid.startswith('osx-')
    library_root = prefix / 'Library' if windows else prefix
    library_dir = library_root / ('bin' if windows else 'lib')
    include = library_root / 'include'
    hdf5 = library_root / 'lib' / ('hdf5.lib' if windows else 'libhdf5.dylib' if mac else 'libhdf5.so')
    sqlite = library_dir / ('sqlite3.dll' if windows else 'libsqlite3.dylib' if mac else 'libsqlite3.so')
    for path in (include / 'hdf5.h', hdf5, sqlite):
        if not path.is_file():
            raise RuntimeError(f'Missing micromamba dependency: {path}; install hdf5 and libsqlite')
    output.mkdir(parents=True, exist_ok=True)
    source = Path(__file__).with_name('raxport_hdf5.c')
    bridge = output / ('raxport_hdf5.dll' if windows else 'libraxport_hdf5.dylib' if mac else 'libraxport_hdf5.so')
    # Link against the shared HDF5 library provided by conda-forge. Never modify
    # libraries in the environment; relocate only copies in the build directory.
    if windows:
        subprocess.run(['cl', '/nologo', '/LD', '/MD', '/O2', '/utf-8', '/DNDEBUG',
                        '/DH5_BUILT_AS_DYNAMIC_LIB', '/D_CRT_SECURE_NO_WARNINGS',
                        f'/I{include}', str(source), f'/Fo:{output / "raxport_hdf5.obj"}',
                        f'/Fe:{bridge}', '/link', '/NOLOGO', str(hdf5)], check=True)
    else:
        compiler = shlex.split(os.environ.get('CC') or ('clang' if mac else 'gcc'))
        args = ['-O3', '-DNDEBUG', '-fPIC', '-dynamiclib' if mac else '-shared']
        if mac:
            args += ['-arch', 'arm64' if rid == 'osx-arm64' else 'x86_64']
        subprocess.run(compiler + args + [str(source), '-o', str(bridge),
                       f'-I{include}', str(hdf5)], check=True)

    dependencies = windows_dependencies if windows else mac_dependencies if mac else linux_dependencies
    # Windows import names are case-insensitive. Include the environment root
    # because conda's VC runtime DLLs can be installed beside python.exe.
    search_dirs = [library_dir, prefix] if windows else [library_dir]
    candidates = {p.name.lower() if windows else p.name: p
                  for directory in reversed(search_dirs) for p in directory.iterdir() if p.is_file()}
    system_linux = {'libc.so.6', 'libm.so.6', 'libdl.so.2', 'libpthread.so.0',
                    'librt.so.1', 'libresolv.so.2', 'libutil.so.1', 'libanl.so.1',
                    'ld-linux-x86-64.so.2'}
    staged = {bridge.name: bridge}
    pending = [bridge]

    def stage(source_path, name):
        key = name.lower() if windows else name
        if key not in staged:
            destination = output / name
            shutil.copy2(source_path, destination)
            destination.chmod(destination.stat().st_mode | 0o200)
            staged[key] = destination
            pending.append(destination)
        return staged[key]

    stage(sqlite, sqlite.name)
    vendor = source.parents[2] / 'dll' / ('timsdata.dll' if windows else 'libtimsdata.so')
    if not mac and vendor.is_file():
        stage(vendor, vendor.name)
    while pending:
        binary = pending.pop()
        for dependency in dependencies(binary):
            name = Path(dependency).name
            if not windows and not mac and name in system_linux:
                continue
            if mac and dependency.startswith(('/usr/lib/', '/System/Library/')):
                continue
            key = name.lower() if windows else name
            if windows and key.startswith(('api-ms-', 'ext-ms-')):
                continue
            candidate = candidates.get(key)
            if candidate is None:
                system_dir = Path(os.environ.get('SystemRoot', 'C:/Windows')) / 'System32'
                if windows and (system_dir / name).is_file():
                    continue
                raise RuntimeError(f'{binary.name}: dependency {dependency} is missing from {library_dir}')
            stage(candidate, name)
            if mac:
                run('install_name_tool', '-change', dependency, f'@loader_path/{name}', binary)
            elif not windows and dependency != name:
                run('patchelf', '--replace-needed', dependency, name, binary)

    for binary in staged.values():
        if mac:
            # Strip only the staged copy and do so before replacing its signature.
            run('strip', '-S', binary)
            run('install_name_tool', '-id', f'@loader_path/{binary.name}', binary)
            # Remove build-machine search paths; all non-system dependencies now
            # resolve beside the library in the .NET extraction directory.
            load_commands = run('otool', '-l', binary)
            for rpath in re.findall(r'cmd LC_RPATH\s+cmdsize \d+\s+path (.*?) \(offset', load_commands):
                run('install_name_tool', '-delete_rpath', rpath, binary)
            run('codesign', '--force', '--sign', '-', binary)
        elif not windows:
            # Conda's compiler runtimes can contain tens of MB of debug symbols.
            # Keep exported dynamic symbols needed by the loader and P/Invoke.
            run(os.environ.get('STRIP', 'strip'), '--strip-unneeded', binary)
            run('patchelf', '--set-rpath', '$ORIGIN', binary)
            if max_glibc:
                check_glibc(binary, max_glibc)
    # MSBuild reads this exact list, excluding compiler .obj/.lib side products.
    (output / 'runtime-files.txt').write_text('\n'.join(str(p) for p in sorted(staged.values())) + '\n')
    print(f'Staged {len(staged)} native libraries from {prefix} in {output}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rid', required=True, choices=['linux-x64', 'osx-x64', 'osx-arm64', 'win-x64'])
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--prefix', type=Path, default=os.environ.get('CONDA_PREFIX'))
    parser.add_argument('--max-glibc')
    args = parser.parse_args()
    if args.prefix is None:
        parser.error('CONDA_PREFIX is not set; run make.sh or make.ps1')
    prepare(args.prefix.resolve(), args.output.resolve(), args.rid, args.max_glibc)


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
