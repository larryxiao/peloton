.PHONY: handin
handin:
	mkdir -p handin
	cp src/backend/index/bwtree_index.cpp handin
	cp src/backend/index/bwtree_index.h   handin
	cp src/backend/index/bwtree.cpp       handin
	cp src/backend/index/bwtree.h         handin
	cp tests/index/index_test.cpp				  handin
	tar -cvf handin.tar handin
stylecheck:
	clang-format-3.6 --style=file ./src/backend/index/bwtree_index.cpp | diff ./src/backend/index/bwtree_index.cpp -
style:
	clang-format-3.6 --style=file -i ./src/backend/index/bwtree_index.cpp

