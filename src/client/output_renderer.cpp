// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "output_renderer.hpp"

#include <algorithm>
#include <iomanip>
#include <sstream>
#include <utility>

#include <arrow/array.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>

#ifdef __unix__
#include <sys/ioctl.h>
#include <unistd.h>
#elif defined(__APPLE__)
#include <sys/ioctl.h>
#include <unistd.h>
#endif

namespace gizmosql::client {

int GetTerminalWidth() {
#if defined(__unix__) || defined(__APPLE__)
  struct winsize ws;
  if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0 && ws.ws_col > 0) {
    return ws.ws_col;
  }
#endif
  return 80;  // default
}

std::string GetCellValue(const std::shared_ptr<arrow::ChunkedArray>& column,
                         int64_t row_index, const std::string& null_value) {
  // Find the right chunk and index within it
  int64_t offset = row_index;
  for (const auto& chunk : column->chunks()) {
    if (offset < chunk->length()) {
      if (chunk->IsNull(offset)) {
        return null_value;
      }
      auto scalar_result = chunk->GetScalar(offset);
      if (scalar_result.ok()) {
        return (*scalar_result)->ToString();
      }
      return null_value;
    }
    offset -= chunk->length();
  }
  return null_value;
}

// Map Arrow type to DuckDB-friendly display name
std::string FriendlyTypeName(const std::shared_ptr<arrow::DataType>& type) {
  switch (type->id()) {
    case arrow::Type::NA:
      return "null";
    case arrow::Type::BOOL:
      return "boolean";
    case arrow::Type::INT8:
      return "tinyint";
    case arrow::Type::INT16:
      return "smallint";
    case arrow::Type::INT32:
      return "integer";
    case arrow::Type::INT64:
      return "bigint";
    case arrow::Type::UINT8:
      return "utinyint";
    case arrow::Type::UINT16:
      return "usmallint";
    case arrow::Type::UINT32:
      return "uinteger";
    case arrow::Type::UINT64:
      return "ubigint";
    case arrow::Type::HALF_FLOAT:
      return "float";
    case arrow::Type::FLOAT:
      return "float";
    case arrow::Type::DOUBLE:
      return "double";
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
      return "varchar";
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_BINARY:
      return "blob";
    case arrow::Type::DATE32:
    case arrow::Type::DATE64:
      return "date";
    case arrow::Type::TIME32:
    case arrow::Type::TIME64:
      return "time";
    case arrow::Type::TIMESTAMP: {
      auto ts_type = std::static_pointer_cast<arrow::TimestampType>(type);
      if (!ts_type->timezone().empty()) {
        return "timestamptz";
      }
      return "timestamp";
    }
    case arrow::Type::DURATION:
      return "interval";
    case arrow::Type::DECIMAL128:
    case arrow::Type::DECIMAL256: {
      auto dec_type = std::static_pointer_cast<arrow::DecimalType>(type);
      return "decimal(" + std::to_string(dec_type->precision()) + "," +
             std::to_string(dec_type->scale()) + ")";
    }
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST:
    case arrow::Type::FIXED_SIZE_LIST:
    case arrow::Type::STRUCT:
    case arrow::Type::MAP:
      return type->ToString();
    default:
      return type->ToString();
  }
}

namespace {

// Helper to compute column widths from data
// show_types: include Arrow type names (for box/table renderers)
// row_limit: only consider this many rows (0 = all)
std::vector<int> ComputeColumnWidths(const arrow::Table& table,
                                     bool show_headers,
                                     const std::string& null_value,
                                     bool show_types = false,
                                     int64_t row_limit = 0) {
  int num_cols = table.num_columns();
  std::vector<int> widths(num_cols, 0);

  if (show_headers) {
    for (int c = 0; c < num_cols; ++c) {
      widths[c] = static_cast<int>(table.schema()->field(c)->name().size());
    }
  }

  if (show_types) {
    for (int c = 0; c < num_cols; ++c) {
      int type_len =
          static_cast<int>(FriendlyTypeName(table.schema()->field(c)->type()).size());
      widths[c] = std::max(widths[c], type_len);
    }
  }

  int64_t rows_to_scan =
      (row_limit > 0 && row_limit < table.num_rows()) ? row_limit : table.num_rows();
  for (int64_t r = 0; r < rows_to_scan; ++r) {
    for (int c = 0; c < num_cols; ++c) {
      int len = static_cast<int>(
          GetCellValue(table.column(c), r, null_value).size());
      widths[c] = std::max(widths[c], len);
    }
  }

  // Ensure minimum width of 1
  for (auto& w : widths) {
    if (w < 1) w = 1;
  }

  return widths;
}

// Cap column widths to fit within max_width, omitting rightmost columns if needed.
// Returns the capped widths and number of columns shown.
std::pair<std::vector<int>, int> CapColumnWidths(std::vector<int> widths,
                                                  int max_width) {
  int num_cols = static_cast<int>(widths.size());
  if (num_cols == 0) return {widths, 0};

  constexpr int kMinColWidth = 4;
  constexpr int kBorderOverhead = 3;  // " " + content + " " + border char

  auto calc_total = [&](int ncols) {
    int total = 1;  // leading border
    for (int c = 0; c < ncols; ++c) {
      total += widths[c] + kBorderOverhead;
    }
    return total;
  };

  // Step 1: Cap individual column widths proportionally
  int available_per_col = (max_width - 1 - num_cols * kBorderOverhead) / num_cols;
  int cap = std::max(kMinColWidth, available_per_col);
  for (auto& w : widths) {
    w = std::min(w, cap);
  }

  // Step 2: Remove rightmost columns if still too wide
  int cols_shown = num_cols;
  while (cols_shown > 1 && calc_total(cols_shown) > max_width) {
    --cols_shown;
  }

  widths.resize(cols_shown);
  return {widths, cols_shown};
}

// Truncate a string value to fit within max_col_width.
// Returns {display_string, display_width}.
std::pair<std::string, int> TruncateValue(const std::string& val,
                                           int max_col_width) {
  int len = static_cast<int>(val.size());
  if (len <= max_col_width) {
    return {val, len};
  }
  if (max_col_width <= 1) {
    return {"\xe2\x80\xa6", 1};  // just ellipsis
  }
  return {val.substr(0, max_col_width - 1) + "\xe2\x80\xa6", max_col_width};
}

// Escape a string for CSV output (RFC 4180)
std::string CsvEscape(const std::string& val) {
  bool needs_quote = false;
  for (char c : val) {
    if (c == '"' || c == ',' || c == '\n' || c == '\r') {
      needs_quote = true;
      break;
    }
  }
  if (!needs_quote) return val;

  std::string result = "\"";
  for (char c : val) {
    if (c == '"') result += "\"\"";
    else result += c;
  }
  result += '"';
  return result;
}

// Escape a string for JSON
std::string JsonEscape(const std::string& val) {
  std::string result;
  for (char c : val) {
    switch (c) {
      case '"':  result += "\\\""; break;
      case '\\': result += "\\\\"; break;
      case '\b': result += "\\b"; break;
      case '\f': result += "\\f"; break;
      case '\n': result += "\\n"; break;
      case '\r': result += "\\r"; break;
      case '\t': result += "\\t"; break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
          result += buf;
        } else {
          result += c;
        }
    }
  }
  return result;
}

// Check if a cell is null in a chunked array
bool IsCellNull(const std::shared_ptr<arrow::ChunkedArray>& col, int64_t row) {
  int64_t offset = row;
  for (const auto& chunk : col->chunks()) {
    if (offset < chunk->length()) return chunk->IsNull(offset);
    offset -= chunk->length();
  }
  return true;
}

// Check if a type is numeric (for JSON rendering and right-alignment)
bool IsNumericType(const std::shared_ptr<arrow::DataType>& type) {
  switch (type->id()) {
    case arrow::Type::INT8:
    case arrow::Type::INT16:
    case arrow::Type::INT32:
    case arrow::Type::INT64:
    case arrow::Type::UINT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT32:
    case arrow::Type::UINT64:
    case arrow::Type::HALF_FLOAT:
    case arrow::Type::FLOAT:
    case arrow::Type::DOUBLE:
    case arrow::Type::DECIMAL128:
    case arrow::Type::DECIMAL256:
      return true;
    default:
      return false;
  }
}

bool IsBooleanType(const std::shared_ptr<arrow::DataType>& type) {
  return type->id() == arrow::Type::BOOL;
}

// Escape for SQL string literals
std::string SqlEscape(const std::string& val) {
  std::string result = "'";
  for (char c : val) {
    if (c == '\'') result += "''";
    else result += c;
  }
  result += "'";
  return result;
}

// Escape for HTML
std::string HtmlEscape(const std::string& val) {
  std::string result;
  for (char c : val) {
    switch (c) {
      case '&':  result += "&amp;"; break;
      case '<':  result += "&lt;"; break;
      case '>':  result += "&gt;"; break;
      case '"':  result += "&quot;"; break;
      default:   result += c;
    }
  }
  return result;
}

// Escape for LaTeX
std::string LatexEscape(const std::string& val) {
  std::string result;
  for (char c : val) {
    switch (c) {
      case '&':  result += "\\&"; break;
      case '%':  result += "\\%"; break;
      case '$':  result += "\\$"; break;
      case '#':  result += "\\#"; break;
      case '_':  result += "\\_"; break;
      case '{':  result += "\\{"; break;
      case '}':  result += "\\}"; break;
      case '~':  result += "\\textasciitilde{}"; break;
      case '^':  result += "\\textasciicircum{}"; break;
      case '\\': result += "\\textbackslash{}"; break;
      default:   result += c;
    }
  }
  return result;
}

// Helper: render a cell value with optional truncation and alignment.
// Writes " content " to out, padded to col_width.
void RenderCell(std::ostream& out, const std::string& val, int col_width,
                bool right_align, const std::string& border) {
  auto [dval, dwidth] = TruncateValue(val, col_width);
  int padding = col_width - dwidth;

  if (right_align) {
    out << " ";
    for (int i = 0; i < padding; ++i) out << " ";
    out << dval << " " << border;
  } else {
    out << " " << dval;
    for (int i = 0; i < padding; ++i) out << " ";
    out << " " << border;
  }
}

// Helper: render a cell value centered within col_width.
void RenderCellCentered(std::ostream& out, const std::string& val, int col_width,
                         const std::string& border) {
  auto [dval, dwidth] = TruncateValue(val, col_width);
  int padding = col_width - dwidth;
  int left_pad = padding / 2;
  int right_pad = padding - left_pad;

  out << " ";
  for (int i = 0; i < left_pad; ++i) out << " ";
  out << dval;
  for (int i = 0; i < right_pad; ++i) out << " ";
  out << " " << border;
}

// Border style for tabular renderers (Box = Unicode, Table = ASCII)
struct BorderStyle {
  const char* tl;  // top-left      ┌ or +
  const char* tr;  // top-right     ┐ or +
  const char* bl;  // bottom-left   └ or +
  const char* br;  // bottom-right  ┘ or +
  const char* h;   // horizontal    ─ or -
  const char* v;   // vertical      │ or |
  const char* tt;  // top-tee       ┬ or +
  const char* bt;  // bottom-tee    ┴ or +
  const char* lt;  // left-tee      ├ or +
  const char* rt;  // right-tee     ┤ or +
  const char* cx;  // cross         ┼ or +
};

const BorderStyle kBoxBorder = {"\u250c", "\u2510", "\u2514", "\u2518",
                                "\u2500", "\u2502", "\u252c", "\u2534",
                                "\u251c", "\u2524", "\u253c"};

const BorderStyle kTableBorder = {"+", "+", "+", "+", "-", "|",
                                  "+", "+", "+", "+", "+"};

// Update column widths by scanning rows in [start, end)
void UpdateWidthsForRows(const arrow::Table& table,
                         const std::string& null_value,
                         std::vector<int>& widths, int64_t start,
                         int64_t end) {
  int num_cols =
      std::min(static_cast<int>(widths.size()), table.num_columns());
  for (int64_t r = start; r < end; ++r) {
    for (int c = 0; c < num_cols; ++c) {
      int len = static_cast<int>(
          GetCellValue(table.column(c), r, null_value).size());
      widths[c] = std::max(widths[c], len);
    }
  }
}

// Shared tabular rendering for Box and Table modes.
// Supports split display (top rows + dot rows + bottom rows),
// in-table footer, column types, and right-aligned numbers.
RenderResult RenderTabular(const arrow::Table& table, std::ostream& out,
                           const BorderStyle& bs, bool show_headers,
                           const std::string& null_value, int max_rows,
                           int max_width) {
  if (table.num_columns() == 0) return {0, 0};

  int64_t total_rows = table.num_rows();
  int total_cols = table.num_columns();

  // Row display strategy
  bool truncating_rows = max_rows > 0 && max_rows < total_rows;
  int64_t rows_to_show = truncating_rows ? max_rows : total_rows;
  bool split_display = truncating_rows && rows_to_show >= 4;

  int64_t top_count = 0, bottom_count = 0, bottom_start = 0;
  if (split_display) {
    top_count = rows_to_show / 2;
    bottom_count = rows_to_show - top_count;
    bottom_start = total_rows - bottom_count;
  } else {
    top_count = rows_to_show;
  }

  // Compute column widths from headers + types + visible rows
  auto widths = ComputeColumnWidths(table, show_headers, null_value,
                                    show_headers, top_count);
  if (split_display) {
    UpdateWidthsForRows(table, null_value, widths, bottom_start, total_rows);
  }

  int cols_shown = total_cols;
  if (max_width > 0) {
    auto [capped, shown] = CapColumnWidths(widths, max_width);
    widths = std::move(capped);
    cols_shown = shown;
  }

  // Numeric column detection for right-alignment
  std::vector<bool> is_numeric(cols_shown, false);
  for (int c = 0; c < cols_shown; ++c) {
    is_numeric[c] = IsNumericType(table.schema()->field(c)->type());
  }

  // Build footer parts early so we can ensure the table is wide enough
  bool cols_truncated = cols_shown < total_cols;
  std::vector<std::string> footer_parts;
  footer_parts.push_back(std::to_string(total_rows) + " row" +
                          (total_rows != 1 ? "s" : ""));
  if (truncating_rows) {
    footer_parts.push_back("(" + std::to_string(rows_to_show) + " shown)");
  }
  if (total_cols > 1 || cols_truncated) {
    footer_parts.push_back(std::to_string(total_cols) + " column" +
                            (total_cols != 1 ? "s" : ""));
    if (cols_truncated) {
      footer_parts.push_back("(" + std::to_string(cols_shown) + " shown)");
    }
  }

  // Find the longest individual footer part (minimum single-line width)
  int max_part_len = 0;
  for (const auto& p : footer_parts) {
    max_part_len = std::max(max_part_len, static_cast<int>(p.size()));
  }

  // Total inner width between outer borders (for merged footer)
  int total_inner_width = 0;
  for (int c = 0; c < cols_shown; ++c) {
    total_inner_width += widths[c] + 2;  // content + padding
  }
  total_inner_width += cols_shown - 1;  // internal borders

  // Widen the last column if footer text would overflow
  int min_footer_inner = max_part_len + 2;  // +2 for padding spaces
  if (min_footer_inner > total_inner_width && cols_shown > 0) {
    int extra = min_footer_inner - total_inner_width;
    widths[cols_shown - 1] += extra;
    total_inner_width = min_footer_inner;
  }

  // --- Lambdas ---

  auto render_border = [&](const char* left, const char* mid,
                           const char* right) {
    out << left;
    for (int c = 0; c < cols_shown; ++c) {
      for (int i = 0; i < widths[c] + 2; ++i) out << bs.h;
      out << (c + 1 < cols_shown ? mid : right);
    }
    out << "\n";
  };

  auto render_data_row = [&](int64_t row_idx) {
    out << bs.v;
    for (int c = 0; c < cols_shown; ++c) {
      std::string val = GetCellValue(table.column(c), row_idx, null_value);
      RenderCell(out, val, widths[c], is_numeric[c], bs.v);
    }
    out << "\n";
  };

  auto render_dot_row = [&]() {
    out << bs.v;
    for (int c = 0; c < cols_shown; ++c) {
      int col_w = widths[c];
      int left_pad = col_w / 2;
      int right_pad = col_w - left_pad - 1;
      out << " ";
      for (int i = 0; i < left_pad; ++i) out << " ";
      out << "\xc2\xb7";  // · (U+00B7, middle dot)
      for (int i = 0; i < right_pad; ++i) out << " ";
      out << " " << bs.v;
    }
    out << "\n";
  };

  // === Render table ===

  render_border(bs.tl, bs.tt, bs.tr);

  if (show_headers) {
    out << bs.v;
    for (int c = 0; c < cols_shown; ++c) {
      RenderCellCentered(out, table.schema()->field(c)->name(), widths[c],
                         bs.v);
    }
    out << "\n";

    out << bs.v;
    for (int c = 0; c < cols_shown; ++c) {
      RenderCellCentered(out,
                         FriendlyTypeName(table.schema()->field(c)->type()),
                         widths[c], bs.v);
    }
    out << "\n";

    render_border(bs.lt, bs.cx, bs.rt);
  }

  // Data rows (split display or sequential)
  if (split_display) {
    for (int64_t r = 0; r < top_count; ++r) render_data_row(r);
    for (int i = 0; i < 3; ++i) render_dot_row();
    for (int64_t r = bottom_start; r < total_rows; ++r) render_data_row(r);
  } else {
    for (int64_t r = 0; r < rows_to_show; ++r) render_data_row(r);
  }

  // In-table footer
  // Merge separator: ├──┴──┴──┤
  render_border(bs.lt, bs.bt, bs.rt);

  // Pack footer parts into lines (greedy, double-space separated)
  int footer_avail = total_inner_width - 2;  // usable text width (1 space padding each side)
  std::vector<std::string> footer_lines;
  std::string current;
  for (const auto& part : footer_parts) {
    if (current.empty()) {
      current = part;
    } else {
      std::string trial = current + "  " + part;
      if (static_cast<int>(trial.size()) <= footer_avail) {
        current = trial;
      } else {
        footer_lines.push_back(current);
        current = part;
      }
    }
  }
  if (!current.empty()) footer_lines.push_back(current);

  // Render each footer line: │ text          │
  for (const auto& fl : footer_lines) {
    int text_len = static_cast<int>(fl.size());
    int rpad = std::max(0, footer_avail - text_len);
    out << bs.v << " " << fl;
    for (int i = 0; i < rpad; ++i) out << " ";
    out << " " << bs.v << "\n";
  }

  // Merged bottom border: └─────────────┘
  out << bs.bl;
  for (int i = 0; i < total_inner_width; ++i) out << bs.h;
  out << bs.br << "\n";

  return {rows_to_show, cols_shown, true};
}

// --- Box Renderer (Unicode box drawing) ---
class BoxRenderer : public OutputRenderer {
 public:
  RenderResult Render(const arrow::Table& table, std::ostream& out) override {
    return RenderTabular(table, out, kBoxBorder, show_headers, null_value,
                         max_rows, max_width);
  }
};

// --- Table Renderer (ASCII borders) ---
class TableRenderer : public OutputRenderer {
 public:
  RenderResult Render(const arrow::Table& table, std::ostream& out) override {
    return RenderTabular(table, out, kTableBorder, show_headers, null_value,
                         max_rows, max_width);
  }
};

// --- CSV Renderer ---
class CsvRenderer : public OutputRenderer {
 public:
  RenderResult Render(const arrow::Table& table, std::ostream& out) override {
    if (show_headers) {
      for (int c = 0; c < table.num_columns(); ++c) {
        if (c > 0) out << ",";
        out << CsvEscape(table.schema()->field(c)->name());
      }
      out << "\n";
    }
    for (int64_t r = 0; r < table.num_rows(); ++r) {
      for (int c = 0; c < table.num_columns(); ++c) {
        if (c > 0) out << ",";
        out << CsvEscape(GetCellValue(table.column(c), r, null_value));
      }
      out << "\n";
    }
    return {table.num_rows(), table.num_columns()};
  }
};

// --- Tabs Renderer ---
class TabsRenderer : public OutputRenderer {
 public:
  RenderResult Render(const arrow::Table& table, std::ostream& out) override {
    if (show_headers) {
      for (int c = 0; c < table.num_columns(); ++c) {
        if (c > 0) out << "\t";
        out << table.schema()->field(c)->name();
      }
      out << "\n";
    }
    for (int64_t r = 0; r < table.num_rows(); ++r) {
      for (int c = 0; c < table.num_columns(); ++c) {
        if (c > 0) out << "\t";
        out << GetCellValue(table.column(c), r, null_value);
      }
      out << "\n";
    }
    return {table.num_rows(), table.num_columns()};
  }
};

// --- JSON Renderer (array of objects) ---
class JsonRenderer : public OutputRenderer {
 public:
  RenderResult Render(const arrow::Table& table, std::ostream& out) override {
    out << "[\n";
    for (int64_t r = 0; r < table.num_rows(); ++r) {
      if (r > 0) out << ",\n";
      out << "  {";
      for (int c = 0; c < table.num_columns(); ++c) {
        if (c > 0) out << ", ";
        std::string name = table.schema()->field(c)->name();
        out << "\"" << JsonEscape(name) << "\": ";

        if (IsCellNull(table.column(c), r)) {
          out << "null";
        } else {
          auto type = table.schema()->field(c)->type();
          std::string val = GetCellValue(table.column(c), r, null_value);
          if (IsNumericType(type)) {
            out << val;
          } else if (IsBooleanType(type)) {
            out << (val == "true" ? "true" : "false");
          } else {
            out << "\"" << JsonEscape(val) << "\"";
          }
        }
      }
      out << "}";
    }
    out << "\n]\n";
    return {table.num_rows(), table.num_columns()};
  }
};

// --- JSON Lines Renderer (NDJSON) ---
class JsonLinesRenderer : public OutputRenderer {
 public:
  RenderResult Render(const arrow::Table& table, std::ostream& out) override {
    for (int64_t r = 0; r < table.num_rows(); ++r) {
      out << "{";
      for (int c = 0; c < table.num_columns(); ++c) {
        if (c > 0) out << ", ";
        std::string name = table.schema()->field(c)->name();
        out << "\"" << JsonEscape(name) << "\": ";

        if (IsCellNull(table.column(c), r)) {
          out << "null";
        } else {
          auto type = table.schema()->field(c)->type();
          std::string val = GetCellValue(table.column(c), r, null_value);
          if (IsNumericType(type)) {
            out << val;
          } else if (IsBooleanType(type)) {
            out << (val == "true" ? "true" : "false");
          } else {
            out << "\"" << JsonEscape(val) << "\"";
          }
        }
      }
      out << "}\n";
    }
    return {table.num_rows(), table.num_columns()};
  }
};

// --- Markdown Renderer ---
class MarkdownRenderer : public OutputRenderer {
 public:
  RenderResult Render(const arrow::Table& table, std::ostream& out) override {
    if (table.num_columns() == 0) return {0, 0};

    auto widths = ComputeColumnWidths(table, true, null_value);

    // Header row
    if (show_headers) {
      out << "|";
      for (int c = 0; c < table.num_columns(); ++c) {
        std::string name = table.schema()->field(c)->name();
        out << " " << name;
        for (int i = static_cast<int>(name.size()); i < widths[c]; ++i)
          out << " ";
        out << " |";
      }
      out << "\n";
    }

    // Separator row (always shown for valid markdown)
    out << "|";
    for (int c = 0; c < table.num_columns(); ++c) {
      out << " ";
      for (int i = 0; i < widths[c]; ++i) out << "-";
      out << " |";
    }
    out << "\n";

    // Data rows
    for (int64_t r = 0; r < table.num_rows(); ++r) {
      out << "|";
      for (int c = 0; c < table.num_columns(); ++c) {
        std::string val = GetCellValue(table.column(c), r, null_value);
        out << " " << val;
        for (int i = static_cast<int>(val.size()); i < widths[c]; ++i)
          out << " ";
        out << " |";
      }
      out << "\n";
    }
    return {table.num_rows(), table.num_columns()};
  }
};

// --- Line Renderer (col = val) ---
class LineRenderer : public OutputRenderer {
 public:
  RenderResult Render(const arrow::Table& table, std::ostream& out) override {
    // Find max column name length for alignment
    int max_name_len = 0;
    for (int c = 0; c < table.num_columns(); ++c) {
      int len = static_cast<int>(table.schema()->field(c)->name().size());
      max_name_len = std::max(max_name_len, len);
    }

    for (int64_t r = 0; r < table.num_rows(); ++r) {
      for (int c = 0; c < table.num_columns(); ++c) {
        std::string name = table.schema()->field(c)->name();
        out << std::setw(max_name_len) << std::right << name << " = "
            << GetCellValue(table.column(c), r, null_value) << "\n";
      }
      if (r + 1 < table.num_rows()) out << "\n";
    }
    return {table.num_rows(), table.num_columns()};
  }
};

// --- List Renderer (configurable separator) ---
class ListRenderer : public OutputRenderer {
 public:
  std::string separator = "|";
  std::string row_separator = "\n";

  RenderResult Render(const arrow::Table& table, std::ostream& out) override {
    if (show_headers) {
      for (int c = 0; c < table.num_columns(); ++c) {
        if (c > 0) out << separator;
        out << table.schema()->field(c)->name();
      }
      out << "\n";
    }
    for (int64_t r = 0; r < table.num_rows(); ++r) {
      for (int c = 0; c < table.num_columns(); ++c) {
        if (c > 0) out << separator;
        out << GetCellValue(table.column(c), r, null_value);
      }
      out << row_separator;
    }
    return {table.num_rows(), table.num_columns()};
  }
};

// --- HTML Renderer ---
class HtmlRenderer : public OutputRenderer {
 public:
  RenderResult Render(const arrow::Table& table, std::ostream& out) override {
    out << "<table>\n";
    if (show_headers) {
      out << "<tr>";
      for (int c = 0; c < table.num_columns(); ++c) {
        out << "<th>" << HtmlEscape(table.schema()->field(c)->name()) << "</th>";
      }
      out << "</tr>\n";
    }
    for (int64_t r = 0; r < table.num_rows(); ++r) {
      out << "<tr>";
      for (int c = 0; c < table.num_columns(); ++c) {
        out << "<td>"
            << HtmlEscape(GetCellValue(table.column(c), r, null_value))
            << "</td>";
      }
      out << "</tr>\n";
    }
    out << "</table>\n";
    return {table.num_rows(), table.num_columns()};
  }
};

// --- LaTeX Renderer ---
class LatexRenderer : public OutputRenderer {
 public:
  RenderResult Render(const arrow::Table& table, std::ostream& out) override {
    if (table.num_columns() == 0) return {0, 0};

    std::string col_spec;
    for (int c = 0; c < table.num_columns(); ++c) {
      col_spec += "l";
    }

    out << "\\begin{tabular}{" << col_spec << "}\n";

    if (show_headers) {
      for (int c = 0; c < table.num_columns(); ++c) {
        if (c > 0) out << " & ";
        out << LatexEscape(table.schema()->field(c)->name());
      }
      out << " \\\\\n";
      out << "\\hline\n";
    }

    for (int64_t r = 0; r < table.num_rows(); ++r) {
      for (int c = 0; c < table.num_columns(); ++c) {
        if (c > 0) out << " & ";
        out << LatexEscape(GetCellValue(table.column(c), r, null_value));
      }
      out << " \\\\\n";
    }

    out << "\\end{tabular}\n";
    return {table.num_rows(), table.num_columns()};
  }
};

// --- Insert Renderer ---
class InsertRenderer : public OutputRenderer {
 public:
  std::string table_name = "table";

  RenderResult Render(const arrow::Table& table, std::ostream& out) override {
    for (int64_t r = 0; r < table.num_rows(); ++r) {
      out << "INSERT INTO " << table_name << " VALUES(";
      for (int c = 0; c < table.num_columns(); ++c) {
        if (c > 0) out << ",";
        if (IsCellNull(table.column(c), r)) {
          out << "NULL";
        } else {
          auto type = table.schema()->field(c)->type();
          std::string val = GetCellValue(table.column(c), r, null_value);
          if (IsNumericType(type)) {
            out << val;
          } else {
            out << SqlEscape(val);
          }
        }
      }
      out << ");\n";
    }
    return {table.num_rows(), table.num_columns()};
  }
};

// --- Quote Renderer ---
class QuoteRenderer : public OutputRenderer {
 public:
  RenderResult Render(const arrow::Table& table, std::ostream& out) override {
    if (show_headers) {
      for (int c = 0; c < table.num_columns(); ++c) {
        if (c > 0) out << ",";
        out << SqlEscape(table.schema()->field(c)->name());
      }
      out << "\n";
    }
    for (int64_t r = 0; r < table.num_rows(); ++r) {
      for (int c = 0; c < table.num_columns(); ++c) {
        if (c > 0) out << ",";
        if (IsCellNull(table.column(c), r)) {
          out << "NULL";
        } else {
          out << SqlEscape(GetCellValue(table.column(c), r, null_value));
        }
      }
      out << "\n";
    }
    return {table.num_rows(), table.num_columns()};
  }
};

// --- ASCII Renderer (unit/record separators) ---
class AsciiRenderer : public OutputRenderer {
 public:
  RenderResult Render(const arrow::Table& table, std::ostream& out) override {
    if (show_headers) {
      for (int c = 0; c < table.num_columns(); ++c) {
        if (c > 0) out << '\x1f';  // unit separator
        out << table.schema()->field(c)->name();
      }
      out << '\x1e';  // record separator
    }
    for (int64_t r = 0; r < table.num_rows(); ++r) {
      for (int c = 0; c < table.num_columns(); ++c) {
        if (c > 0) out << '\x1f';
        out << GetCellValue(table.column(c), r, null_value);
      }
      out << '\x1e';
    }
    return {table.num_rows(), table.num_columns()};
  }
};

// --- Trash Renderer (discard output) ---
class TrashRenderer : public OutputRenderer {
 public:
  RenderResult Render(const arrow::Table& table, std::ostream& /*out*/) override {
    return {table.num_rows(), table.num_columns()};
  }
};

}  // anonymous namespace

std::unique_ptr<OutputRenderer> CreateRenderer(OutputMode mode,
                                                const ClientConfig& config) {
  std::unique_ptr<OutputRenderer> renderer;

  switch (mode) {
    case OutputMode::BOX:
      renderer = std::make_unique<BoxRenderer>();
      break;
    case OutputMode::TABLE:
      renderer = std::make_unique<TableRenderer>();
      break;
    case OutputMode::CSV:
      renderer = std::make_unique<CsvRenderer>();
      break;
    case OutputMode::TABS:
      renderer = std::make_unique<TabsRenderer>();
      break;
    case OutputMode::JSON:
      renderer = std::make_unique<JsonRenderer>();
      break;
    case OutputMode::JSONLINES:
      renderer = std::make_unique<JsonLinesRenderer>();
      break;
    case OutputMode::MARKDOWN:
      renderer = std::make_unique<MarkdownRenderer>();
      break;
    case OutputMode::LINE:
      renderer = std::make_unique<LineRenderer>();
      break;
    case OutputMode::LIST: {
      auto list = std::make_unique<ListRenderer>();
      list->separator = config.separator_col;
      list->row_separator = config.separator_row;
      renderer = std::move(list);
      break;
    }
    case OutputMode::HTML:
      renderer = std::make_unique<HtmlRenderer>();
      break;
    case OutputMode::LATEX:
      renderer = std::make_unique<LatexRenderer>();
      break;
    case OutputMode::INSERT: {
      auto insert = std::make_unique<InsertRenderer>();
      insert->table_name = config.insert_table;
      renderer = std::move(insert);
      break;
    }
    case OutputMode::QUOTE:
      renderer = std::make_unique<QuoteRenderer>();
      break;
    case OutputMode::ASCII:
      renderer = std::make_unique<AsciiRenderer>();
      break;
    case OutputMode::TRASH:
      renderer = std::make_unique<TrashRenderer>();
      break;
  }

  renderer->show_headers = config.show_headers;
  renderer->null_value = config.null_value;
  renderer->max_rows = config.max_rows;
  renderer->max_width = config.max_width;
  return renderer;
}

}  // namespace gizmosql::client
