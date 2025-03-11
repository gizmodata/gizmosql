#pragma once

#include "arrow/record_batch.h"

namespace gizmosql::ddb {

/// \brief Gets the hard-coded type info from Sqlite for all data types.
/// \return A record batch.
arrow::Result<std::shared_ptr<arrow::RecordBatch>> DoGetTypeInfoResult();

/// \brief Gets the hard-coded type info from Sqlite filtering
///        for a specific data type.
/// \return A record batch.
arrow::Result<std::shared_ptr<arrow::RecordBatch>> DoGetTypeInfoResult(
    int data_type_filter);

}  // namespace gizmosql::ddb
