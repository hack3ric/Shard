#!/bin/bash
if [ -d "build" ]; then
  rm -r build
fi
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j
cp ../patch/libmlx5.so libmlx5.so
echo 'Build completed'
