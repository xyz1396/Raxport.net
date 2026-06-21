
### compile on linux

```bash
sudo apt-get update && sudo apt-get install -y dotnet-sdk-8.0
# aot gives errors on thermo dll
# dotnet publish -c Release -r linux-x64 -p:PublishAot=true --self-contained true
dotnet publish RaxportNetCore.csproj /p:PublishProfile=linux-single
# to void dotnet ram isssue on linux
# ERROR - Command execution failed: GC heap initialization failed with error 0x8007000E
# Failed to create CoreCLR, HRESULT: 0x8007000E
export DOTNET_GCHeapHardLimit=200000000

```