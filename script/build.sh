#!/bin/bash

# 设置目标目录
TARGET_DIR="./target"

# 检查是否存在 target 目录，如果存在则清除
echo "Cleaning up old build files..."
rm -rf $TARGET_DIR
mkdir -p $TARGET_DIR

# 打印一些信息
echo "Building project..."

# 编译调试版本
echo "Building in debug mode..."
cargo build --verbose

# 检查是否有错误
if [ $? -ne 0 ]; then
    echo "Debug build failed!"
    exit 1
fi

# 编译发布版本
echo "Building in release mode..."
cargo build --release --verbose

# 检查是否有错误
if [ $? -ne 0 ]; then
    echo "Release build failed!"
    exit 1
fi

# 输出构建的可执行文件路径
echo "Debug build output: $TARGET_DIR/debug/$(basename $PWD)"
echo "Release build output: $TARGET_DIR/release/$(basename $PWD)"

# 可选：运行发布版构建的可执行文件
# echo "Running release build..."
# $TARGET_DIR/release/$(basename $PWD)

# 提示完成
echo "Build completed successfully."
