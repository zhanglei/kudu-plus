@echo off
color 2e
chcp 65001

@title bat交互执行git命令演示

REM 临时附加git.exe到path环境变量，止处也可以手动加到“环境变量”控制面板，永久生效
set path=%path%;D:\java\Git\bin

REM 切换到工程目录
echo\&echo switching project directory
cd /d %~dp0

REM echo new file>file.txt
REM cd.>README.md

git add -v .
git commit -m "auto-commit %date% %time%"

git push gitee
git push github

echo\&echo done...
pause