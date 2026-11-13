Param(
    [Parameter(Mandatory = $true)]
    [string]$InputFile = $null,

    [Parameter(Mandatory = $true)]
    [string]$OutputPath = $null
)

Write-Host "InputFile : $InputFile"
Write-Host "OutputPath : $OutputPath"

$regexPattern = "^ *\d+ +[0-9A-F]+ [0-9A-F]+ (rocksdb_[a-zA-Z_0-9]+).*$"

$extractedValues = @()
$extractedValues += "LIBRARY ROCKSJNI"
$extractedValues += "EXPORTS"

$fileContent = Get-Content -Path $InputFile
foreach ($line in $fileContent)
{
    if ($line -match $regexPattern)
    {
        $extractedData = $Matches[1]
        $extractedValues += $extractedData
    }
}
$extractedValues | Out-File -FilePath $OutputPath -Encoding utf8
Write-Host "Extraction completed. Extracted values are stored in $OutputPath."
