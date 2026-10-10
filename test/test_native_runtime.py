"""Dependency-closure checks independent of the host's native toolchain."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    'prepare_runtime', Path(__file__).resolve().parents[1] / 'src/native/prepare_runtime.py')
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


class NativeRuntimeTests(unittest.TestCase):
    def exercise(self, rid, missing=False):
        windows = rid == 'win-x64'
        mac = rid.startswith('osx-')
        with tempfile.TemporaryDirectory(prefix='raxport runtime ') as directory:
            root = Path(directory)
            prefix, output = root / 'env', root / 'out'
            library_root = prefix / 'Library' if windows else prefix
            libraries = library_root / ('bin' if windows else 'lib')
            libraries.mkdir(parents=True)
            (library_root / 'include').mkdir()
            (library_root / 'include/hdf5.h').touch()
            if windows:
                (library_root / 'lib').mkdir()
                (library_root / 'lib/hdf5.lib').touch()
            hdf5 = 'hdf5.dll' if windows else 'libhdf5.dylib' if mac else 'libhdf5.so'
            sqlite = 'sqlite3.dll' if windows else 'libsqlite3.dylib' if mac else 'libsqlite3.so'
            compression = 'ZLIB.dll' if windows else 'libz.1.dylib' if mac else 'libz.so.1'
            for name in (hdf5, sqlite, compression):
                (libraries / name).write_text('original environment library')
            bridge = 'raxport_hdf5.dll' if windows else 'libraxport_hdf5.dylib' if mac else 'libraxport_hdf5.so'
            system = 'api-ms-win-crt-runtime-l1-1-0.dll' if windows else '/usr/lib/libSystem.B.dylib' if mac else 'libc.so.6'
            def reference(name):
                return f'@rpath/{name}' if mac else name.lower() if windows else name
            graph = {
                bridge: [reference(hdf5), system],
                hdf5: [reference(compression)],
                sqlite: [reference(compression)],
                # A cycle and a repeated dependency must be handled once.
                compression.lower() if windows else compression: [reference(hdf5)],
                'libtimsdata.so': [], 'timsdata.dll': [],
            }
            if missing:
                graph[hdf5] = [reference('missing-library')]
            calls = []
            def command(*args):
                calls.append(args)
                return ''
            def compile_bridge(*args, **kwargs):
                (output / bridge).touch()
            def dependencies(path):
                return graph[path.name]
            host = ('Windows', 'AMD64') if windows else ('Darwin', 'arm64') if mac else ('Linux', 'x86_64')
            dependency_function = 'windows_dependencies' if windows else 'mac_dependencies' if mac else 'linux_dependencies'
            with patch.object(runtime.platform, 'system', return_value=host[0]), \
                 patch.object(runtime.platform, 'machine', return_value=host[1]), \
                 patch.object(runtime.subprocess, 'run', side_effect=compile_bridge), \
                 patch.object(runtime, dependency_function, side_effect=dependencies), \
                 patch.object(runtime, 'run', side_effect=command):
                if missing:
                    with self.assertRaisesRegex(RuntimeError, 'missing-library'):
                        runtime.prepare(prefix, output, rid, None)
                    self.assertFalse((output / 'runtime-files.txt').exists())
                    return
                runtime.prepare(prefix, output, rid, None)
            files = (output / 'runtime-files.txt').read_text().splitlines()
            names = {Path(path).name.lower() for path in files}
            self.assertTrue({bridge.lower(), hdf5.lower(), sqlite.lower(), compression.lower()} <= names)
            self.assertEqual(len(files), len(names))
            for name in (hdf5, sqlite, compression):
                self.assertEqual((libraries / name).read_text(), 'original environment library')
            if mac:
                self.assertIn(('install_name_tool', '-change', reference(hdf5),
                               f'@loader_path/{hdf5}', output / bridge), calls)
                self.assertEqual(sum(call[0] == 'codesign' for call in calls), len(files))
                for path in map(Path, files):
                    self.assertLess(calls.index(('strip', '-S', path)),
                                    calls.index(('codesign', '--force', '--sign', '-', path)))
            elif not windows:
                self.assertEqual(sum(call[:3] == ('patchelf', '--set-rpath', '$ORIGIN')
                                     for call in calls), len(files))
                for path in map(Path, files):
                    self.assertLess(calls.index((runtime.os.environ.get('STRIP', 'strip'),
                                                 '--strip-unneeded', path)),
                                    calls.index(('patchelf', '--set-rpath', '$ORIGIN', path)))

    def test_linux_dependency_closure(self):
        self.exercise('linux-x64')

    def test_macos_relocation_and_signing(self):
        self.exercise('osx-arm64')

    def test_windows_case_insensitive_dependencies(self):
        self.exercise('win-x64')

    def test_missing_dependency_fails_before_manifest(self):
        self.exercise('linux-x64', missing=True)

    def test_rejects_glibc_newer_than_release_baseline(self):
        with patch.object(runtime, 'run', return_value='GLIBC_2.2.5 GLIBC_2.28'):
            with self.assertRaisesRegex(RuntimeError, 'GLIBC_2.28'):
                runtime.check_glibc(Path('test.so'), '2.17')


if __name__ == '__main__':
    unittest.main()
