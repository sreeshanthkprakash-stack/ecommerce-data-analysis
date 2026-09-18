Add-Type -AssemblyName System.Runtime.WindowsRuntime

$asTaskGeneric = ([System.WindowsRuntimeSystemExtensions].GetMethods() | ? { 
    $_.Name -eq 'AsTask' -and 
    $_.IsGenericMethodDefinition -and 
    $_.GetParameters().Count -eq 1 
})[0]

$asTaskAction = ([System.WindowsRuntimeSystemExtensions].GetMethods() | ? { 
    $_.Name -eq 'AsTask' -and 
    -not $_.IsGenericMethodDefinition -and 
    $_.GetParameters().Count -eq 1 -and 
    $_.GetParameters()[0].ParameterType.Name -eq 'IAsyncAction' 
})[0]

function Await($op, [Type]$type) {
    if ($type) {
        $m = $asTaskGeneric.MakeGenericMethod($type)
        return $m.Invoke($null, @($op)).GetAwaiter().GetResult()
    } else {
        $asTaskAction.Invoke($null, @($op)).GetAwaiter().GetResult()
    }
}

[Windows.Data.Pdf.PdfDocument, Windows.Data.Pdf, ContentType = WindowsRuntime] | Out-Null

$pdfPath = (Resolve-Path "ecommerce.pdf").Path
$outputDir = Join-Path (Get-Location) "images"
if (!(Test-Path $outputDir)) { 
    New-Item -ItemType Directory -Path $outputDir | Out-Null 
}

$fileStream = [System.IO.File]::OpenRead($pdfPath)
$winRtStream = [System.IO.WindowsRuntimeStreamExtensions]::AsRandomAccessStream($fileStream)

$pdfDoc = Await ([Windows.Data.Pdf.PdfDocument]::LoadFromStreamAsync($winRtStream)) ([Windows.Data.Pdf.PdfDocument])

Write-Host "Total Pages: $($pdfDoc.PageCount)"

$names = @(
    "01_executive_overview.png",
    "02_sales_revenue.png",
    "03_customer_analytics.png",
    "04_product_analytics.png",
    "05_time_analysis.png",
    "06_marketing_channel_analytics.png",
    "07_device_payment_analytics.png",
    "08_funnel_conversion.png"
)

for ($i = 0; $i -lt $pdfDoc.PageCount; $i++) {
    $page = $pdfDoc.GetPage($i)
    $filename = $names[$i]
    $outPath = Join-Path $outputDir $filename
    
    $outStream = [System.IO.File]::Open($outPath, [System.IO.FileMode]::Create)
    $winRtOutStream = [System.IO.WindowsRuntimeStreamExtensions]::AsRandomAccessStream($outStream)
    
    $renderOptions = [Windows.Data.Pdf.PdfPageRenderOptions]::new()
    $renderOptions.DestinationWidth = 1920
    
    Await ($page.RenderToStreamAsync($winRtOutStream, $renderOptions)) $null
    
    Await ($winRtOutStream.FlushAsync()) ([bool]) | Out-Null
    $winRtOutStream.Dispose()
    $outStream.Dispose()
    
    Write-Host "Exported: images/$filename"
}

$winRtStream.Dispose()
$fileStream.Dispose()
Write-Host "All 8 images successfully generated!"
