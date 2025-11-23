# Starts ngrok on port 5000 from `C:\ngrok\ngrok.exe`.
#
# Usage: run this script from PowerShell; it will open a new window
# for the ngrok session so your terminal is free for the Flask server.

$ngrokPath = "C:\ngrok\ngrok.exe"

if (Test-Path $ngrokPath) {
    Start-Process -FilePath $ngrokPath -ArgumentList "http 5000" -WindowStyle Normal
} else {
    Write-Error "Ngrok executable not found at $ngrokPath. Please install ngrok or adjust the path."
}
