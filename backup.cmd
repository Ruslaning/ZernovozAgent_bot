@echo off
cd /d C:\Users\Kseni\zernovoz-agent
git add -A >nul 2>&1
git commit -m "Auto backup %date% %time%" --quiet >nul 2>&1
git push origin main --quiet >nul 2>&1