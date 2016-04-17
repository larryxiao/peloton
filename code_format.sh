#!/bin/bash
files=( src/backend/wire tests/wire )
for dir in "${files[@]}"
do
	find "$dir" -name '*.h' -or -name '*.cpp' -print0 | xargs -0 clang-format-3.6 --style=file -i
done

for dir in "${files[@]}"
do
	find "$dir" \( -name '*.h' -or -name '*.cpp' \) -print0 | xargs -0I {} sh -c \
	"echo {}; clang-format-3.6 --style=file {} | diff {} -"
done