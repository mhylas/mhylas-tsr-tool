$listener = New-Object System.Net.HttpListener
$listener.Prefixes.Add('http://localhost:3000/')
$listener.Start()
Write-Host "Serving on http://localhost:3000/"
$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
while ($listener.IsListening) {
    $ctx = $listener.GetContext()
    $req = $ctx.Request
    $res = $ctx.Response
    $path = $req.Url.LocalPath.TrimStart('/')
    if ($path -eq '' -or $path -eq '/') { $path = 'tsr-dashboard.html' }
    $file = Join-Path $dir $path
    if (Test-Path $file) {
        $ext = [System.IO.Path]::GetExtension($file)
        $mime = if ($ext -eq '.html') { 'text/html' } elseif ($ext -eq '.js') { 'application/javascript' } elseif ($ext -eq '.css') { 'text/css' } else { 'application/octet-stream' }
        $res.ContentType = $mime
        $bytes = [System.IO.File]::ReadAllBytes($file)
        $res.ContentLength64 = $bytes.Length
        $res.OutputStream.Write($bytes, 0, $bytes.Length)
    } else {
        $res.StatusCode = 404
    }
    $res.Close()
}
