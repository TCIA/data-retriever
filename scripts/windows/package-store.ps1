# Build an unsigned x64 MSIX for Partner Center (the Store signs accepted packages).
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidatePattern('^\d+\.\d+\.\d+\.0$')][string]$Version,
    [Parameter(Mandatory)][ValidatePattern('^[A-Za-z0-9.-]{3,50}$')][string]$IdentityName,
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$Publisher,
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$PublisherDisplayName
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if (@($Version.Split('.') | Where-Object { [long]$_ -gt 65535 }).Count -gt 0) {
    throw 'MSIX version components must be between 0 and 65535.'
}
$repo = (Resolve-Path "$PSScriptRoot/../..").Path
$binary = Join-Path $repo 'build/bin/TCIA_Data_Retriever.exe'
if (!(Test-Path $binary)) { throw "Build the Windows Wails application first: $binary" }
$sdkRoot = Join-Path ${env:ProgramFiles(x86)} 'Windows Kits/10/bin'
$makeAppx = Get-ChildItem "$sdkRoot/*/x64/makeappx.exe" |
    Sort-Object { [version]$_.Directory.Parent.Name } -Descending | Select-Object -First 1
if (!$makeAppx) { throw 'Install the Windows 10/11 SDK with MakeAppx.exe.' }
$staging = Join-Path $repo 'build/bin/store-staging'
$output = Join-Path $repo 'build/bin/store'
foreach ($path in @($staging, $output)) {
    if (Test-Path $path) { Remove-Item $path -Recurse -Force }
    New-Item $path -ItemType Directory | Out-Null
}
New-Item "$staging/Assets" -ItemType Directory | Out-Null
Copy-Item $binary $staging

# Reuse the app's icon, preserving its aspect ratio on transparent square canvases.
Add-Type -AssemblyName System.Drawing
$source = [System.Drawing.Image]::FromFile((Join-Path $repo 'build/appicon.png'))
try {
    foreach ($asset in @(@('StoreLogo', 50), @('Square44x44Logo', 44), @('Square150x150Logo', 150))) {
        $size = [int]$asset[1]
        $bitmap = [System.Drawing.Bitmap]::new($size, $size)
        $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
        try {
            $graphics.Clear([System.Drawing.Color]::Transparent)
            $graphics.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
            $scale = [Math]::Min($size / $source.Width, $size / $source.Height)
            $width = [int]($source.Width * $scale)
            $height = [int]($source.Height * $scale)
            $graphics.DrawImage($source, [int](($size - $width) / 2), [int](($size - $height) / 2), $width, $height)
            $bitmap.Save("$staging/Assets/$($asset[0]).png", [System.Drawing.Imaging.ImageFormat]::Png)
        } finally {
            $graphics.Dispose()
            $bitmap.Dispose()
        }
    }
} finally { $source.Dispose() }

[xml]$manifest = Get-Content (Join-Path $repo 'build/windows/store/AppxManifest.xml') -Raw
$manifest.Package.Identity.SetAttribute('Name', $IdentityName)
$manifest.Package.Identity.SetAttribute('Publisher', $Publisher)
$manifest.Package.Identity.SetAttribute('Version', $Version)
$manifest.Package.Properties.PublisherDisplayName = $PublisherDisplayName
$manifest.Save((Join-Path $staging 'AppxManifest.xml'))
$package = Join-Path $output "TCIA_Data_Retriever_${Version}_x64.msix"
# Keep MakeAppx's default schema and semantic validation enabled (no /nv).
& $makeAppx.FullName pack /d $staging /p $package /o
if ($LASTEXITCODE -ne 0) { throw 'MakeAppx validation/packaging failed.' }
Get-FileHash $package -Algorithm SHA256 | Format-List
Write-Host "Unsigned Store package: $package"
