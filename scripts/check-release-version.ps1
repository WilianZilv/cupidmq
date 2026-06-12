# Verify git tag vX.Y.Z matches master/Cargo.toml and python-client/pyproject.toml
param(
    [Parameter(Mandatory = $true)]
    [string]$Tag
)

if ($Tag -notmatch '^v\d+\.\d+\.\d+') {
    Write-Error "tag must look like v0.1.0 (got '$Tag')"
    exit 1
}

$Version = $Tag.Substring(1)
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)

function Get-VersionFromToml($Path) {
    $line = Get-Content $Path | Where-Object { $_ -match '^\s*version\s*=' } | Select-Object -First 1
    if ($line -match '"([^"]+)"') { return $Matches[1] }
    Write-Error "no version in $Path"
    exit 1
}

$CargoVer = Get-VersionFromToml (Join-Path $Root "master\Cargo.toml")
$PyVer = Get-VersionFromToml (Join-Path $Root "python-client\pyproject.toml")

$fail = $false
if ($CargoVer -ne $Version) {
    Write-Error "master/Cargo.toml version=$CargoVer, expected $Version (tag $Tag)"
    $fail = $true
}
if ($PyVer -ne $Version) {
    Write-Error "python-client/pyproject.toml version=$PyVer, expected $Version (tag $Tag)"
    $fail = $true
}
if ($fail) { exit 1 }

Write-Host "ok: tag $Tag matches crate=$CargoVer python=$PyVer"
