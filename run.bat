@echo off
echo Launching Redis and 3 Go server instances...

:: Start Redis Client
start "Redis CLI" cmd /k "redis-cli -p 6379"

:: Start Go Instance 0
start "Server Instance 0" cmd /k "go run . 0"

:: Start Go Instance 1
start "Server Instance 1" cmd /k "go run . 1"

:: Start Go Instance 2
start "Server Instance 2" cmd /k "go run . 2"

echo All instances triggered!
pause
