#!/bin/bash
# 编译脚本 - 自动使用C++11标准

if [ $# -eq 0 ]; then
    echo "用法: $0 <源文件.cpp>"
    echo "示例: $0 test.cpp"
    exit 1
fi

source_file=$1
output_name="${source_file%.cpp}"

echo "编译 $source_file 使用 C++11 标准..."
g++ -std=c++11 -Wall -Wextra "$source_file" -o "$output_name"

if [ $? -eq 0 ]; then
    echo "编译成功！可执行文件: $output_name"
else
    echo "编译失败！"
    exit 1
fi 