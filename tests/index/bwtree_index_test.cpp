//
// Created by siddharth on 22/2/16.
//
#include "gtest/gtest.h"
#include "harness.h"

#include "backend/common/logger.h"
#include "backend/index/index_factory.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace test {

	catalog::Schema *key_schema = nullptr;
	catalog::Schema *tuple_schema = nullptr;

	ItemPointer item0(120, 5);
	ItemPointer item1(120, 7);
	ItemPointer item2(123, 19);

	index::Index *BuildIndex() {
		// Build tuple and key schema
		std::vector<std::vector<std::string>> column_names;
		std::vector<catalog::Column> columns;
		std::vector<catalog::Schema *> schemas;
		IndexType index_type = INDEX_TYPE_BWTREE;
		// TODO: Uncomment the line below
		//index_type = INDEX_TYPE_BWTREE;

		catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
														"A", true);
		catalog::Column column2(VALUE_TYPE_VARCHAR, 1024, "B", true);
		catalog::Column column3(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE),
														"C", true);
		catalog::Column column4(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
														"D", true);

		columns.push_back(column1);
		columns.push_back(column2);

		// INDEX KEY SCHEMA -- {column1, column2}
		key_schema = new catalog::Schema(columns);
		key_schema->SetIndexedColumns({0, 1});

		columns.push_back(column3);
		columns.push_back(column4);

		// TABLE SCHEMA -- {column1, column2, column3, column4}
		tuple_schema = new catalog::Schema(columns);

		// Build index metadata
		const bool unique_keys = false;

		index::IndexMetadata *index_metadata = new index::IndexMetadata(
				"test_index", 125, index_type, INDEX_CONSTRAINT_TYPE_DEFAULT,
				tuple_schema, key_schema, unique_keys);

		// Build index
		index::Index *index = index::IndexFactory::GetInstance(index_metadata);
		EXPECT_TRUE(index != NULL);

		return index;
	}

	TEST(IndexTests, BasicTest) {
		auto pool = TestingHarness::GetInstance().GetTestingPool();
		std::vector<ItemPointer> locations;

		// INDEX
		std::unique_ptr<index::Index> index(BuildIndex());

		std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));

		key0->SetValue(0, ValueFactory::GetIntegerValue(100), pool);

		key0->SetValue(1, ValueFactory::GetStringValue("a"), pool);

		// INSERT
		index->InsertEntry(key0.get(), item0);

//		locations = index->ScanKey(key0.get());
//		EXPECT_EQ(locations.size(), 1);
//		EXPECT_EQ(locations[0].block, item0.block);
//
//		// DELETE
//		index->DeleteEntry(key0.get(), item0);
//
//		locations = index->ScanKey(key0.get());
//		EXPECT_EQ(locations.size(), 0);

		delete tuple_schema;
	}

}
}

