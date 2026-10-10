#requires -Version 5.1
<#
.SYNOPSIS
Build and test the Windows x64 single-file executable in bin/.
.DESCRIPTION
Requires micromamba in PATH and an x64 Visual Studio Developer PowerShell
with the Desktop development with C++ workload and Windows SDK installed.
Run this command yourself once before running .\make.ps1:
  micromamba create -y -n raxport -c conda-forge --platform win-64 --strict-channel-priority dotnet-sdk=8 python=3.12 h5py numpy matplotlib pyteomics lxml "hdf5=*=nompi_*" libsqlite zlib libaec pefile
Python packages also support test/visualize_raxport_hdf5.py, including mzML input.
The script automatically runs inside raxport; no manual activation is needed.
Only the final executable is written to bin/; intermediate files and test results go to build/.
Run .\make.ps1 clean (or -Clean) to remove bin/ and build/ without build tools.
Managed packages restore through nupkg/NuGet.config: local ThermoFisher
packages (version pinned in the project), with other dependencies from nuget.org.
HDF5, SQLite, and their transitive DLLs are copied from the environment
and bundled into the single-file executable.
AOT publish is not used because of Thermo DLL/native interop errors.

Optional runtime tuning (not applied during the build):
  $env:DOTNET_GCHeapHardLimit = "0x1000000000"
In Windows CMD: set DOTNET_GCHeapHardLimit=0x1000000000
Heap limits per process: 0x100000000=4 GiB, 0x200000000=8 GiB,
0x400000000=16 GiB, 0x800000000=32 GiB, 0x1000000000=64 GiB,
0x2000000000=128 GiB.
One -p unit buffers 10,000,000 peak rows before HDF5 flush; default -p 2
buffers about 20M rows. Use a smaller -p for small heaps or many files with
-j; increase the heap limit before increasing -p. Examples:
  .\bin\Raxport-win-x64.exe -i 'input path' -o 'output path' -j 6 -p 1
  $env:DOTNET_GCHeapHardLimit = "0x1000000000"
  .\bin\Raxport-win-x64.exe -i 'input path' -o 'output path' -j 6 -p 2
  $env:DOTNET_GCHeapHardLimit = "0x2000000000"
  .\bin\Raxport-win-x64.exe -i 'input path' -o 'output path' -j 6 -p 3
--hdf5-compression-level 1 (default) favors speed with gzip enabled;
use 6 for smaller output or 0 to disable compression for faster writes.
.EXAMPLE
.\make.ps1
.EXAMPLE
.\make.ps1 clean
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('build', 'clean')]
    [string]$Action = 'build',
    [switch]$Clean,
    [switch]$Help,
    [switch]$BuildInEnvironment
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Write-Step([string]$Message) { Write-Host "`n==> $Message" }
function Clear-BuildOutput {
    Write-Step 'Cleaning generated build and publish directories'
    foreach ($directory in @('build', 'bin')) {
        $path = Join-Path $PSScriptRoot $directory
        if (Test-Path -LiteralPath $path) { Remove-Item -LiteralPath $path -Recurse -Force }
    }
}
function Invoke-Native {
    param([string]$Command, [string[]]$Arguments)
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Command failed with exit code $LASTEXITCODE"
    }
}
if ($Help) {
    Get-Help $PSCommandPath -Detailed
    exit 0
}
if (-not (Test-Path -LiteralPath (Join-Path $PSScriptRoot 'src/RaxportNetCore.csproj'))) {
    throw 'Could not identify the repository root.'
}
if ($Clean -or $Action -eq 'clean') {
    Clear-BuildOutput
    exit 0
}
if ($env:OS -ne 'Windows_NT') { throw 'Use ./make.sh on Linux or macOS.' }
if (-not (Get-Command cl.exe -ErrorAction SilentlyContinue) -or $env:VSCMD_ARG_TGT_ARCH -ne 'x64') {
    throw 'Run from an x64 Visual Studio Developer PowerShell with the C++ workload and Windows SDK installed.'
}

if (-not $BuildInEnvironment) {
    if (-not (Get-Command micromamba -ErrorAction SilentlyContinue)) {
        throw 'micromamba is required but was not found in PATH.'
    }
    $environmentName = 'raxport'
    $envJson = & micromamba env list --json
    if ($LASTEXITCODE -ne 0) { throw 'Could not list micromamba environments.' }
    $environments = ($envJson -join "`n") | ConvertFrom-Json
    $exists = @($environments.envs | Where-Object {
        (Split-Path -Leaf $_) -eq $environmentName
    }).Count -gt 0
    if (-not $exists) {
        throw "Missing environment $environmentName; create it using the command at the top of make.ps1."
    }
    Write-Step "Using micromamba environment $environmentName"
    $powershell = (Get-Process -Id $PID).Path
    Invoke-Native 'micromamba' @(
        'run', '-n', $environmentName, $powershell, '-NoProfile',
        '-File', $PSCommandPath, '-BuildInEnvironment'
    )
    exit 0
}
if (-not $env:CONDA_PREFIX) { throw 'micromamba did not activate the build environment.' }

$originalEnvironment = @{}
foreach ($name in @('PATH', 'TMP', 'TEMP', 'TMPDIR', 'DOTNET_BUNDLE_EXTRACT_BASE_DIR')) {
    $originalEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}
Push-Location $PSScriptRoot
try {
    Write-Step 'Checking micromamba native dependencies'
    foreach ($relativePath in @('Library/include/hdf5.h', 'Library/lib/hdf5.lib', 'Library/bin/hdf5.dll', 'Library/bin/sqlite3.dll')) {
        $dependency = Join-Path $env:CONDA_PREFIX $relativePath
        if (-not (Test-Path -LiteralPath $dependency -PathType Leaf)) {
            throw "Missing dependency: $dependency; install the packages listed at the top of make.ps1."
        }
    }
    Clear-BuildOutput
    $tempDirectory = Join-Path $PSScriptRoot 'build/tmp'
    New-Item -ItemType Directory -Force -Path $tempDirectory | Out-Null
    $env:TMP = $tempDirectory
    $env:TEMP = $tempDirectory
    $env:TMPDIR = $tempDirectory
    $env:DOTNET_BUNDLE_EXTRACT_BASE_DIR = Join-Path $PSScriptRoot 'build/bundle'

    Write-Step 'Restoring managed packages from nupkg/ and nuget.org'
    Invoke-Native 'dotnet' @(
        'restore', 'src/RaxportNetCore.csproj',
        '--configfile', (Join-Path $PSScriptRoot 'nupkg/NuGet.config'),
        '/p:PublishProfile=win-x64-single-file'
    )
    Write-Step 'Publishing the win-x64 single-file executable'
    Invoke-Native 'dotnet' @(
        'publish', 'src/RaxportNetCore.csproj', '--no-restore',
        '/p:PublishProfile=win-x64-single-file', '-p:UseSharedCompilation=false'
    )
    $bridge = 'build/native/win-x64/raxport_hdf5.dll'
    $app = 'bin/Raxport-win-x64.exe'
    foreach ($artifact in @($bridge, $app)) {
        if (-not (Test-Path $artifact -PathType Leaf)) { throw "Missing build artifact: $artifact" }
    }
    foreach ($looseLibrary in @('bin/raxport_hdf5.dll', 'bin/sqlite3.dll')) {
        if (Test-Path $looseLibrary) { throw "Native library was not bundled into the executable: $looseLibrary" }
    }

    Write-Step 'Running tests'
    Invoke-Native 'python' @('-B', '-m', 'unittest', 'discover', '-s', 'test', '-p', 'test_native_runtime.py')
    $env:PATH = "$(Join-Path $PSScriptRoot 'build/native/win-x64');$env:PATH"
    Invoke-Native 'dotnet' @(
        'restore', 'src/RaxportNetCore.csproj',
        '--configfile', (Join-Path $PSScriptRoot 'nupkg/NuGet.config'), '-p:EnableRaxportTests=true'
    )
    Invoke-Native 'dotnet' @(
        'test', 'src/RaxportNetCore.csproj', '--no-restore', '-p:EnableRaxportTests=true',
        '-p:UseSharedCompilation=false', '--results-directory', (Join-Path $PSScriptRoot 'build/TestResults'),
        '--logger', 'console;verbosity=minimal'
    )
    Write-Step 'Running CLI smoke test'
    Invoke-Native (Join-Path $PSScriptRoot $app) @('-h')
    $publishedFiles = @(Get-ChildItem -LiteralPath (Join-Path $PSScriptRoot 'bin') -Force)
    if ($publishedFiles.Count -ne 1 -or $publishedFiles[0].Name -ne 'Raxport-win-x64.exe') {
        throw 'bin/ must contain only the final Raxport-win-x64.exe executable.'
    }
    Write-Host "`nBuild complete: $(Join-Path $PSScriptRoot $app)"
} finally {
    foreach ($name in $originalEnvironment.Keys) {
        [Environment]::SetEnvironmentVariable($name, $originalEnvironment[$name], 'Process')
    }
    Pop-Location
}
