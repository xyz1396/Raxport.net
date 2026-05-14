
### compile on linux

```bash
sudo apt-get update && sudo apt-get install -y dotnet-sdk-8.0
rm -r bin obj Properties TestResults .vs
dotnet restore
# aot gives errors on thermo dll
# dotnet publish -c Release -r linux-x64 -p:PublishAot=true --self-contained true
dotnet publish -c Release -r linux-x64 -p:PublishSingleFile=true -p:PublishTrimmed=true --self-contained true
dotnet publish -c Release -r win-x64 -p:PublishSingleFile=true -p:PublishTrimmed=true --self-contained true
cp bin/Release/net8.0/linux-x64/publish/Raxport bin
cp bin/Release/net8.0/win-x64/publish/Raxport.exe bin
# to void dotnet ram isssue on linux
# ERROR - Command execution failed: GC heap initialization failed with error 0x8007000E
# Failed to create CoreCLR, HRESULT: 0x8007000E
export DOTNET_GCHeapHardLimit=200000000

```