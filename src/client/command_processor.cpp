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

#include "command_processor.hpp"

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <vector>

#ifdef _WIN32
#include <direct.h>
#define chdir _chdir
#else
#include <unistd.h>
#endif

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/flight/sql/types.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

#include "output_renderer.hpp"
#include "password_prompt.hpp"

namespace gizmosql::client {

namespace {

// ANSI red for error messages (matching DuckDB's error styling)
constexpr const char* kErrRed = "\033[1;31m";
constexpr const char* kErrReset = "\033[0m";

std::vector<std::string> SplitArgs(const std::string& line) {
  std::vector<std::string> args;
  std::istringstream iss(line);
  std::string token;
  while (iss >> token) {
    args.push_back(token);
  }
  return args;
}

std::string ToLower(const std::string& s) {
  std::string result = s;
  std::transform(result.begin(), result.end(), result.begin(), ::tolower);
  return result;
}

void RenderTable(const std::shared_ptr<arrow::Table>& table,
                 ClientConfig& config) {
  auto renderer = CreateRenderer(config.output_mode, config);
  renderer->Render(*table, *config.output_stream);
}

// Extract a string value from a SqlInfo result table for a given info_name key.
// The table has columns: info_name (uint32), value (dense union).
// Returns empty string if not found or on error.
std::string ExtractSqlInfoString(const std::shared_ptr<arrow::Table>& table,
                                 int info_name) {
  if (!table || table->num_rows() == 0 || table->num_columns() < 2) return "";

  auto name_col = table->column(0);   // uint32
  auto value_col = table->column(1);   // dense union

  for (int chunk_idx = 0; chunk_idx < name_col->num_chunks(); ++chunk_idx) {
    auto name_array =
        std::static_pointer_cast<arrow::UInt32Array>(name_col->chunk(chunk_idx));
    auto union_array =
        std::static_pointer_cast<arrow::DenseUnionArray>(value_col->chunk(chunk_idx));

    for (int64_t r = 0; r < name_array->length(); ++r) {
      if (static_cast<int>(name_array->Value(r)) == info_name) {
        // Type code 0 = string in the SqlInfo union schema
        int8_t type_code = union_array->type_code(r);
        if (type_code == 0) {
          auto str_array = std::static_pointer_cast<arrow::StringArray>(
              union_array->field(0));
          return str_array->GetString(union_array->value_offset(r));
        }
        return "";
      }
    }
  }
  return "";
}

}  // namespace

bool CommandProcessor::IsDotCommand(const std::string& line) {
  size_t pos = line.find_first_not_of(" \t");
  return pos != std::string::npos && line[pos] == '.';
}

CommandResult CommandProcessor::Process(const std::string& line) {
  auto args = SplitArgs(line);
  if (args.empty()) return CommandResult::OK;

  std::string cmd = ToLower(args[0]);

  // .quit / .exit
  if (cmd == ".quit" || cmd == ".exit") {
    return CommandResult::EXIT;
  }

  // .connect HOST PORT USERNAME [PASSWORD]
  if (cmd == ".connect") {
    return HandleConnect(args);
  }

  // .help [PATTERN]
  if (cmd == ".help") {
    ShowHelp(args.size() > 1 ? args[1] : "");
    return CommandResult::OK;
  }

  // .tables [PATTERN] [--flat]
  if (cmd == ".tables") {
    if (!conn_.IsConnected()) {
      std::cerr << kErrRed << "Error: not connected to a server. Use '.connect HOST PORT "
                   "USERNAME' to connect."
                << kErrReset << std::endl;
      return CommandResult::ERROR;
    }
    std::string pattern;
    bool flat_mode = false;
    for (size_t i = 1; i < args.size(); ++i) {
      if (args[i] == "--flat" || args[i] == "-f") {
        flat_mode = true;
      } else {
        pattern = args[i];
      }
    }

    // Request include_schema for the rich box view
    auto result = conn_.GetTables("", "", pattern.empty() ? "" : "%" + pattern + "%",
                                   !flat_mode);
    if (!result.ok()) {
      std::cerr << kErrRed << "Error: " << result.status().ToString() << kErrReset << std::endl;
      return CommandResult::OK;
    }

    if (flat_mode) {
      RenderTable(*result, config_);
      return CommandResult::OK;
    }

    auto& table = *result;
    if (table->num_rows() == 0) {
      *config_.output_stream << "No tables found." << std::endl;
      return CommandResult::OK;
    }

    // Columns: catalog_name(0), db_schema_name(1), table_name(2),
    //          table_type(3), table_schema(4) [binary, serialized Arrow schema]
    bool has_schema_col = table->num_columns() >= 5;

    // Struct to hold parsed table info
    struct TableBox {
      std::string catalog;
      std::string schema;
      std::string name;
      std::vector<std::pair<std::string, std::string>> columns;  // name, type
      int64_t row_count = -1;  // -1 = unknown
    };

    // Shorten type name for display (e.g., "decimal(15,2)" -> "decimal")
    auto short_type = [](const std::string& type_name) -> std::string {
      auto paren = type_name.find('(');
      if (paren != std::string::npos) return type_name.substr(0, paren);
      return type_name;
    };

    // Parse all tables
    std::vector<TableBox> all_boxes;
    for (int64_t r = 0; r < table->num_rows(); ++r) {
      TableBox box;
      box.catalog = GetCellValue(table->column(0), r, "");
      box.schema = GetCellValue(table->column(1), r, "");
      box.name = GetCellValue(table->column(2), r, "");

      // Deserialize the Arrow schema from the binary column
      if (has_schema_col) {
        // Find the correct chunk and local index
        int64_t offset = r;
        for (int ci = 0; ci < table->column(4)->num_chunks(); ++ci) {
          auto chunk = table->column(4)->chunk(ci);
          if (offset < chunk->length()) {
            auto bin_arr = std::static_pointer_cast<arrow::BinaryArray>(chunk);
            if (bin_arr->IsValid(offset)) {
              auto view = bin_arr->GetView(offset);
              auto buf = std::make_shared<arrow::Buffer>(
                  reinterpret_cast<const uint8_t*>(view.data()),
                  static_cast<int64_t>(view.size()));
              arrow::io::BufferReader reader(buf);
              arrow::ipc::DictionaryMemo dict_memo;
              auto schema_result = arrow::ipc::ReadSchema(&reader, &dict_memo);
              if (schema_result.ok()) {
                for (int f = 0; f < (*schema_result)->num_fields(); ++f) {
                  box.columns.emplace_back(
                      (*schema_result)->field(f)->name(),
                      short_type(FriendlyTypeName((*schema_result)->field(f)->type())));
                }
              }
            }
            break;
          }
          offset -= chunk->length();
        }
      }

      all_boxes.push_back(std::move(box));
    }

    // Fetch row counts in a single UNION ALL query
    if (!all_boxes.empty()) {
      std::ostringstream count_sql;
      for (size_t i = 0; i < all_boxes.size(); ++i) {
        if (i > 0) count_sql << " UNION ALL ";
        // Quote table name to handle special characters
        std::string qualified;
        if (!all_boxes[i].schema.empty()) {
          qualified = "\"" + all_boxes[i].schema + "\".";
        }
        qualified += "\"" + all_boxes[i].name + "\"";
        count_sql << "SELECT '" << all_boxes[i].name
                  << "' AS t, COUNT(*) AS c FROM " << qualified;
      }
      auto count_result = conn_.ExecuteQuery(count_sql.str());
      if (count_result.ok() && count_result->table->num_rows() > 0) {
        auto& ct = count_result->table;
        for (int64_t cr = 0; cr < ct->num_rows(); ++cr) {
          std::string tname = GetCellValue(ct->column(0), cr, "");
          std::string cval = GetCellValue(ct->column(1), cr, "0");
          for (auto& box : all_boxes) {
            if (box.name == tname && box.row_count < 0) {
              try { box.row_count = std::stoll(cval); } catch (...) {}
              break;
            }
          }
        }
      }
    }

    // Group by catalog > schema (preserving order from server)
    std::map<std::string, std::map<std::string, std::vector<size_t>>> tree;
    for (size_t i = 0; i < all_boxes.size(); ++i) {
      tree[all_boxes[i].catalog][all_boxes[i].schema].push_back(i);
    }

    int term_width = GetTerminalWidth();
    auto& out = *config_.output_stream;

    // ANSI color codes matching DuckDB's shell palette
    const char* kOrange = "\033[38;5;172m";     // DATABASE_NAME / catalog
    const char* kBlue = "\033[38;5;39m";        // SCHEMA_NAME
    const char* kBold = "\033[1m";              // TABLE_NAME
    const char* kReset = "\033[0m";
    const char* kGray = "\033[90m";             // LAYOUT, FOOTER, COLUMN_TYPE

    // Render a centered catalog header:  ──── label ────  (orange)
    auto render_catalog_header = [&](const std::string& label) {
      std::string padded = " " + label + " ";
      int pad = term_width - static_cast<int>(padded.size()) - 2;
      int lp = pad / 2;
      int rp = pad - lp;
      if (lp < 1) lp = 1;
      if (rp < 1) rp = 1;
      out << kGray << " ";
      for (int i = 0; i < lp; ++i) out << "\u2500";
      out << kReset << kOrange << kBold << padded << kReset << kGray;
      for (int i = 0; i < rp; ++i) out << "\u2500";
      out << " " << kReset << "\n";
    };

    // Render a centered schema header:  ──── label ────  (blue)
    auto render_schema_header = [&](const std::string& label) {
      std::string padded = " " + label + " ";
      int pad = term_width - static_cast<int>(padded.size()) - 2;
      int lp = pad / 2;
      int rp = pad - lp;
      if (lp < 1) lp = 1;
      if (rp < 1) rp = 1;
      out << kGray << " ";
      for (int i = 0; i < lp; ++i) out << "\u2500";
      out << kReset << kBlue << kBold << padded << kReset << kGray;
      for (int i = 0; i < rp; ++i) out << "\u2500";
      out << " " << kReset << "\n";
    };

    for (auto& [catalog, schemas] : tree) {
      render_catalog_header(catalog);

      for (auto& [schema, indices] : schemas) {
        render_schema_header(schema);

        // Sort tables by column count descending (tallest first, like DuckDB)
        std::sort(indices.begin(), indices.end(),
                  [&](size_t a, size_t b) {
                    return all_boxes[a].columns.size() > all_boxes[b].columns.size();
                  });

        // Pre-render each box into lines
        struct RenderedBox {
          int width;  // inner width (excluding │ borders)
          std::vector<std::string> lines;
          size_t box_idx;  // index into all_boxes for re-rendering
        };

        // Render a box at a given width
        auto render_box = [&](size_t box_idx, int w) -> RenderedBox {
          auto& box = all_boxes[box_idx];
          RenderedBox rb;
          rb.width = w;
          rb.box_idx = box_idx;

          // Helper to center text within rb.width
          auto center = [&](const std::string& text, int visual_len) {
            int pad = w - visual_len;
            int lp = pad / 2;
            int rp = pad - lp;
            std::string line(lp, ' ');
            line += text;
            line += std::string(rp, ' ');
            return line;
          };

          // Table name (centered, bold)
          {
            std::string styled = std::string(kBold) + box.name + kReset;
            rb.lines.push_back(center(styled, static_cast<int>(box.name.size())));
          }

          // Blank separator
          rb.lines.push_back(std::string(w, ' '));

          // Column lines: " name     type "
          for (auto& [cn, ct] : box.columns) {
            std::string name_part = " " + cn;
            int target = w - static_cast<int>(ct.size()) - 1;
            while (static_cast<int>(name_part.size()) < target) name_part += ' ';
            std::string line = name_part + kGray + ct + kReset + " ";
            rb.lines.push_back(line);
          }

          // Row count footer (centered, gray)
          std::string row_count_str;
          if (box.row_count >= 0) {
            row_count_str = std::to_string(box.row_count) + " row" +
                            (box.row_count != 1 ? "s" : "");
          }
          if (!row_count_str.empty()) {
            rb.lines.push_back(std::string(w, ' '));
            std::string styled = std::string(kGray) + row_count_str + kReset;
            rb.lines.push_back(
                center(styled, static_cast<int>(row_count_str.size())));
          }

          return rb;
        };

        // Initial render at natural width
        std::vector<RenderedBox> rendered;
        for (auto box_idx : indices) {
          auto& box = all_boxes[box_idx];

          // Compute minimum width
          int max_cname = 0, max_ctype = 0;
          for (auto& [cn, ct] : box.columns) {
            max_cname = std::max(max_cname, static_cast<int>(cn.size()));
            max_ctype = std::max(max_ctype, static_cast<int>(ct.size()));
          }
          std::string row_count_str;
          if (box.row_count >= 0) {
            row_count_str = std::to_string(box.row_count) + " row" +
                            (box.row_count != 1 ? "s" : "");
          }
          int col_content = max_cname + 1 + max_ctype;
          int inner = std::max({static_cast<int>(box.name.size()),
                                col_content,
                                static_cast<int>(row_count_str.size())});
          int w = inner + 2;  // 1 space padding each side

          rendered.push_back(render_box(box_idx, w));
        }

        // --- Masonry layout ---
        // Assign boxes to columns. Each column tracks its current height.
        int num_cols = 0;
        {
          int used = 0;
          for (auto& rb : rendered) {
            int bw = rb.width + 2;  // +2 for │ borders
            if (used + bw > term_width && num_cols > 0) break;
            used += bw;
            ++num_cols;
          }
          if (num_cols == 0) num_cols = 1;
        }

        // Each column slot holds a queue of box indices
        std::vector<std::vector<size_t>> col_slots(num_cols);
        // Assign boxes to the shortest column
        std::vector<int> col_heights(num_cols, 0);
        for (size_t bi = 0; bi < rendered.size(); ++bi) {
          // Find the shortest column
          int min_col = 0;
          for (int c = 1; c < num_cols; ++c) {
            if (col_heights[c] < col_heights[min_col]) min_col = c;
          }
          col_slots[min_col].push_back(bi);
          // +2 for top/bottom border, +content lines
          col_heights[min_col] += static_cast<int>(rendered[bi].lines.size()) + 2;
        }

        // Normalize: all boxes in the same column must have the same width.
        // Re-render narrower boxes at the column's max width so that table
        // names, row counts, and column lines are properly centered/aligned.
        std::vector<int> col_widths(num_cols, 0);
        for (int c = 0; c < num_cols; ++c) {
          for (auto bi : col_slots[c]) {
            col_widths[c] = std::max(col_widths[c], rendered[bi].width);
          }
          for (auto bi : col_slots[c]) {
            if (rendered[bi].width < col_widths[c]) {
              rendered[bi] = render_box(rendered[bi].box_idx, col_widths[c]);
            }
          }
        }

        // Render the masonry: process row by row across all columns
        // Each column has a queue of boxes. We render one row of pixels at a time.
        struct ColState {
          size_t queue_idx = 0;    // which box in the queue we're currently rendering
          int line_in_box = -1;   // -1 = top border, lines.size() = bottom border
          bool done = false;
        };
        std::vector<ColState> states(num_cols);

        auto all_done = [&]() {
          for (auto& s : states)
            if (!s.done) return false;
          return true;
        };

        while (!all_done()) {
          for (int c = 0; c < num_cols; ++c) {
            auto& st = states[c];
            if (st.done) {
              // Fill with spaces for this column width
              if (st.queue_idx > 0) {
                auto last_bi = col_slots[c][st.queue_idx - 1];
                out << std::string(rendered[last_bi].width + 2, ' ');
              }
              continue;
            }

            auto bi = col_slots[c][st.queue_idx];
            auto& rb = rendered[bi];

            if (st.line_in_box == -1) {
              // Top border (gray)
              out << kGray << "\u250C";
              for (int i = 0; i < rb.width; ++i) out << "\u2500";
              out << "\u2510" << kReset;
              st.line_in_box = 0;
            } else if (st.line_in_box < static_cast<int>(rb.lines.size())) {
              // Content line
              out << kGray << "\u2502" << kReset
                  << rb.lines[st.line_in_box]
                  << kGray << "\u2502" << kReset;
              ++st.line_in_box;
            } else {
              // Bottom border (gray)
              out << kGray << "\u2514";
              for (int i = 0; i < rb.width; ++i) out << "\u2500";
              out << "\u2518" << kReset;
              // Move to next box in this column
              ++st.queue_idx;
              if (st.queue_idx < col_slots[c].size()) {
                st.line_in_box = -1;
              } else {
                st.done = true;
              }
            }
          }
          out << "\n";
        }
      }
    }
    return CommandResult::OK;
  }

  // .schema [PATTERN]
  if (cmd == ".schema") {
    if (!conn_.IsConnected()) {
      std::cerr << kErrRed << "Error: not connected to a server. Use '.connect HOST PORT "
                   "USERNAME' to connect."
                << kErrReset << std::endl;
      return CommandResult::ERROR;
    }
    std::string pattern = args.size() > 1 ? args[1] : "";
    auto result = conn_.GetDbSchemas("", pattern.empty() ? "" : "%" + pattern + "%");
    if (result.ok()) {
      RenderTable(*result, config_);
    } else {
      std::cerr << kErrRed << "Error: " << result.status().ToString() << kErrReset << std::endl;
    }
    return CommandResult::OK;
  }

  // .catalogs
  if (cmd == ".catalogs") {
    if (!conn_.IsConnected()) {
      std::cerr << kErrRed << "Error: not connected to a server. Use '.connect HOST PORT "
                   "USERNAME' to connect."
                << kErrReset << std::endl;
      return CommandResult::ERROR;
    }
    auto result = conn_.GetCatalogs();
    if (result.ok()) {
      RenderTable(*result, config_);
    } else {
      std::cerr << kErrRed << "Error: " << result.status().ToString() << kErrReset << std::endl;
    }
    return CommandResult::OK;
  }

  // .mode MODE
  if (cmd == ".mode") {
    if (args.size() < 2) {
      *config_.output_stream << "current mode: "
                              << OutputModeToString(config_.output_mode)
                              << std::endl;
      return CommandResult::OK;
    }
    auto mode = OutputModeFromString(args[1]);
    if (mode.has_value()) {
      config_.output_mode = *mode;
    } else {
      std::cerr << kErrRed << "Error: unknown mode '" << args[1] << "'. Available modes:\n"
                << "  table box csv tabs json jsonlines markdown line list\n"
                << "  html latex insert quote ascii trash" << kErrReset << std::endl;
    }
    return CommandResult::OK;
  }

  // .headers on|off
  if (cmd == ".headers") {
    if (args.size() < 2) {
      *config_.output_stream << "headers: "
                              << (config_.show_headers ? "on" : "off")
                              << std::endl;
    } else {
      config_.show_headers = (ToLower(args[1]) == "on");
    }
    return CommandResult::OK;
  }

  // .timer on|off
  if (cmd == ".timer") {
    if (args.size() < 2) {
      *config_.output_stream << "timer: "
                              << (config_.show_timer ? "on" : "off")
                              << std::endl;
    } else {
      config_.show_timer = (ToLower(args[1]) == "on");
    }
    return CommandResult::OK;
  }

  // .output [FILE]
  if (cmd == ".output") {
    if (args.size() < 2 || args[1] == "stdout") {
      // Reset to stdout
      if (output_file_) {
        delete output_file_;
        output_file_ = nullptr;
      }
      config_.output_stream = &std::cout;
    } else {
      if (output_file_) {
        delete output_file_;
      }
      output_file_ = new std::ofstream(args[1]);
      if (!output_file_->is_open()) {
        std::cerr << kErrRed << "Error: cannot open file '" << args[1] << "'" << kErrReset << std::endl;
        delete output_file_;
        output_file_ = nullptr;
      } else {
        config_.output_stream = output_file_;
      }
    }
    return CommandResult::OK;
  }

  // .once FILE
  if (cmd == ".once") {
    if (args.size() < 2) {
      std::cerr << kErrRed << "Usage: .once FILE" << kErrReset << std::endl;
    } else {
      if (output_file_) {
        delete output_file_;
      }
      output_file_ = new std::ofstream(args[1]);
      if (!output_file_->is_open()) {
        std::cerr << kErrRed << "Error: cannot open file '" << args[1] << "'" << kErrReset << std::endl;
        delete output_file_;
        output_file_ = nullptr;
      } else {
        config_.output_stream = output_file_;
        once_mode_ = true;
      }
    }
    return CommandResult::OK;
  }

  // .nullvalue STRING
  if (cmd == ".nullvalue") {
    if (args.size() < 2) {
      *config_.output_stream << "nullvalue: \"" << config_.null_value << "\""
                              << std::endl;
    } else {
      config_.null_value = args[1];
    }
    return CommandResult::OK;
  }

  // .separator COL [ROW]
  if (cmd == ".separator") {
    if (args.size() < 2) {
      *config_.output_stream << "separator: \"" << config_.separator_col
                              << "\" \"" << config_.separator_row << "\""
                              << std::endl;
    } else {
      config_.separator_col = args[1];
      if (args.size() > 2) {
        config_.separator_row = args[2];
      }
    }
    return CommandResult::OK;
  }

  // .show
  if (cmd == ".show") {
    ShowSettings();
    return CommandResult::OK;
  }

  // .echo on|off
  if (cmd == ".echo") {
    if (args.size() < 2) {
      *config_.output_stream << "echo: " << (config_.echo ? "on" : "off")
                              << std::endl;
    } else {
      config_.echo = (ToLower(args[1]) == "on");
    }
    return CommandResult::OK;
  }

  // .bail on|off
  if (cmd == ".bail") {
    if (args.size() < 2) {
      *config_.output_stream << "bail: "
                              << (config_.bail_on_error ? "on" : "off")
                              << std::endl;
    } else {
      config_.bail_on_error = (ToLower(args[1]) == "on");
    }
    return CommandResult::OK;
  }

  // .maxrows [N]
  if (cmd == ".maxrows") {
    if (args.size() < 2) {
      *config_.output_stream << "maxrows: " << config_.max_rows << std::endl;
    } else {
      try {
        config_.max_rows = std::stoi(args[1]);
      } catch (...) {
        std::cerr << kErrRed << "Error: invalid number '" << args[1] << "'" << kErrReset << std::endl;
      }
    }
    return CommandResult::OK;
  }

  // .maxwidth [N]
  if (cmd == ".maxwidth") {
    if (args.size() < 2) {
      *config_.output_stream << "maxwidth: " << config_.max_width << std::endl;
    } else {
      try {
        int val = std::stoi(args[1]);
        if (val == 0) {
          config_.max_width = GetTerminalWidth();
          config_.auto_width = true;
        } else {
          config_.max_width = val;
          config_.auto_width = false;
        }
      } catch (...) {
        std::cerr << kErrRed << "Error: invalid number '" << args[1] << "'" << kErrReset << std::endl;
      }
    }
    return CommandResult::OK;
  }

  // .read FILE
  if (cmd == ".read") {
    if (args.size() < 2) {
      std::cerr << kErrRed << "Usage: .read FILE" << kErrReset << std::endl;
      return CommandResult::OK;
    }
    // This is handled in the shell loop since it needs SQL processor context
    // For now, just print a message
    std::cerr << kErrRed << "Error: .read is handled by the shell loop" << kErrReset << std::endl;
    return CommandResult::OK;
  }

  // .shell CMD...
  if (cmd == ".shell") {
    if (args.size() < 2) {
      std::cerr << kErrRed << "Usage: .shell CMD..." << kErrReset << std::endl;
    } else {
      std::string shell_cmd;
      for (size_t i = 1; i < args.size(); ++i) {
        if (i > 1) shell_cmd += " ";
        shell_cmd += args[i];
      }
      std::system(shell_cmd.c_str());
    }
    return CommandResult::OK;
  }

  // .cd DIR
  if (cmd == ".cd") {
    if (args.size() < 2) {
      // cd to home
#ifdef _WIN32
      const char* home = std::getenv("USERPROFILE");
#else
      const char* home = std::getenv("HOME");
#endif
      if (home) chdir(home);
    } else {
      if (chdir(args[1].c_str()) != 0) {
        std::cerr << kErrRed << "Error: cannot change to directory '" << args[1] << "'"
                  << kErrReset << std::endl;
      }
    }
    return CommandResult::OK;
  }

  // .prompt MAIN [CONT]
  if (cmd == ".prompt") {
    if (args.size() >= 2) {
      config_.main_prompt = args[1] + " ";
    }
    if (args.size() >= 3) {
      config_.continuation_prompt = args[2] + " ";
    }
    return CommandResult::OK;
  }

  // .refresh
  if (cmd == ".refresh") {
    if (refresh_callback_) refresh_callback_();
    *config_.output_stream << "Schema cache refreshed." << std::endl;
    return CommandResult::OK;
  }

  // .describe TABLE
  if (cmd == ".describe") {
    if (!conn_.IsConnected()) {
      std::cerr << kErrRed << "Error: not connected to a server." << kErrReset << std::endl;
      return CommandResult::ERROR;
    }
    if (args.size() < 2) {
      std::cerr << kErrRed << "Usage: .describe TABLE" << kErrReset << std::endl;
      return CommandResult::ERROR;
    }
    std::string sql = "DESCRIBE " + args[1];
    auto result = conn_.ExecuteQuery(sql);
    if (result.ok()) {
      RenderTable(result->table, config_);
    } else {
      std::cerr << kErrRed << "Error: " << result.status().ToString() << kErrReset << std::endl;
    }
    return CommandResult::OK;
  }

  // .highlight on|off
  if (cmd == ".highlight") {
    if (args.size() < 2) {
      *config_.output_stream << "highlight: "
                              << (config_.syntax_highlighting ? "on" : "off")
                              << std::endl;
    } else {
      config_.syntax_highlighting = (ToLower(args[1]) == "on");
    }
    return CommandResult::OK;
  }

  // .last — re-display cached last result
  if (cmd == ".last") {
    if (!last_result_ || !*last_result_) {
      std::cerr << kErrRed << "No cached result. Run a SELECT query first." << kErrReset << std::endl;
      return CommandResult::ERROR;
    }
    RenderTable(*last_result_, config_);
    return CommandResult::OK;
  }

  // .export_last [FILE]
  if (cmd == ".export_last") {
    if (!last_result_ || !*last_result_) {
      std::cerr << kErrRed << "No cached result. Run a SELECT query first." << kErrReset << std::endl;
      return CommandResult::ERROR;
    }
    std::string path;
    if (args.size() >= 2) {
      path = args[1];
    } else {
#ifdef _WIN32
      const char* home = std::getenv("USERPROFILE");
#else
      const char* home = std::getenv("HOME");
#endif
      path = home ? std::string(home) + "/.gizmosql_last_result.arrow"
                  : ".gizmosql_last_result.arrow";
    }

    auto out_file = arrow::io::FileOutputStream::Open(path);
    if (!out_file.ok()) {
      std::cerr << kErrRed << "Error: " << out_file.status().ToString() << kErrReset << std::endl;
      return CommandResult::ERROR;
    }
    auto writer = arrow::ipc::MakeFileWriter(out_file->get(),
                                              (*last_result_)->schema());
    if (!writer.ok()) {
      std::cerr << kErrRed << "Error: " << writer.status().ToString() << kErrReset << std::endl;
      return CommandResult::ERROR;
    }
    auto table_reader = arrow::TableBatchReader(**last_result_);
    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
      auto status = table_reader.ReadNext(&batch);
      if (!status.ok()) {
        std::cerr << kErrRed << "Error: " << status.ToString() << kErrReset << std::endl;
        break;
      }
      if (!batch) break;
      auto ws = (*writer)->WriteRecordBatch(*batch);
      if (!ws.ok()) {
        std::cerr << kErrRed << "Error: " << ws.ToString() << kErrReset << std::endl;
        break;
      }
    }
    auto close_status = (*writer)->Close();
    if (!close_status.ok()) {
      std::cerr << kErrRed << "Error: " << close_status.ToString() << kErrReset << std::endl;
      return CommandResult::ERROR;
    }
    *config_.output_stream << "Exported " << (*last_result_)->num_rows()
                            << " rows to " << path << std::endl;
    return CommandResult::OK;
  }

  // .pager on|off
  if (cmd == ".pager") {
    if (args.size() < 2) {
      *config_.output_stream << "pager: "
                              << (config_.pager_enabled ? "on" : "off")
                              << " (threshold: " << config_.pager_threshold
                              << " rows)" << std::endl;
    } else if (ToLower(args[1]) == "on" || ToLower(args[1]) == "off") {
      config_.pager_enabled = (ToLower(args[1]) == "on");
    } else {
      // Treat as threshold number
      try {
        config_.pager_threshold = std::stoi(args[1]);
      } catch (...) {
        std::cerr << kErrRed << "Usage: .pager on|off|N" << kErrReset << std::endl;
      }
    }
    return CommandResult::OK;
  }

  std::cerr << kErrRed << "Error: unknown command '" << cmd
            << "'. Type '.help' for available commands." << kErrReset << std::endl;
  return CommandResult::ERROR;
}

void CommandProcessor::SetRefreshCallback(std::function<void()> callback) {
  refresh_callback_ = std::move(callback);
}

void CommandProcessor::SetPromptRefreshCallback(std::function<void()> callback) {
  prompt_refresh_callback_ = std::move(callback);
}

void CommandProcessor::SetLastResult(std::shared_ptr<arrow::Table>* last_result) {
  last_result_ = last_result;
}

void CommandProcessor::ShowHelp(const std::string& pattern) {
  struct HelpEntry {
    std::string command;
    std::string description;
  };

  std::vector<HelpEntry> entries = {
      {".bail on|off", "Stop on error. Default: off"},
      {".catalogs", "List all catalogs"},
      {".cd DIR", "Change working directory"},
      {".connect URI | HOST PORT USER",
       "Connect to a GizmoSQL server"},
      {".describe TABLE", "Show table column names and types"},
      {".echo on|off", "Echo input commands. Default: off"},
      {".exit", "Exit this program (same as .quit)"},
      {".export_last [FILE]", "Export last result to Arrow IPC file"},
      {".headers on|off", "Toggle column headers. Default: on"},
      {".help [PATTERN]", "Show this help or commands matching PATTERN"},
      {".highlight on|off", "Toggle syntax highlighting. Default: on"},
      {".last", "Re-display last query result"},
      {".maxrows [N]", "Show or set max rows displayed (0=unlimited)"},
      {".maxwidth [N]", "Show or set max width (0=auto from terminal)"},
      {".mode MODE", "Set output mode (box table csv json markdown ...)"},
      {".nullvalue STRING", "Set string for NULL values. Default: \"NULL\""},
      {".once FILE", "Redirect next query output to FILE"},
      {".output [FILE]", "Redirect output to FILE or stdout"},
      {".pager on|off|N", "Toggle pager or set row threshold. Default: on/50"},
      {".prompt MAIN [CONT]", "Set prompt strings"},
      {".quit", "Exit this program"},
      {".read FILE", "Execute SQL from FILE"},
      {".refresh", "Refresh tab-completion schema cache"},
      {".schema [PATTERN]", "Show database schemas"},
      {".separator COL [ROW]", "Set separators for list/csv mode"},
      {".shell CMD...", "Execute CMD in system shell"},
      {".show", "Show current settings"},
      {".tables [PATTERN] [--flat]", "List tables (tree view, or --flat)"},
      {".timer on|off", "Show query execution time. Default: off"},
  };

  std::string filter = ToLower(pattern);
  auto& out = *config_.output_stream;

  for (const auto& entry : entries) {
    if (filter.empty() || entry.command.find(filter) != std::string::npos ||
        entry.description.find(filter) != std::string::npos) {
      out << std::left << std::setw(25) << entry.command << entry.description
          << "\n";
    }
  }
}

void CommandProcessor::ShowSettings() {
  auto& out = *config_.output_stream;

  // --- Server ---
  if (conn_.IsConnected()) {
    out << "--- Server ---\n";

    // Fetch engine and arrow version from FlightSQL SqlInfo metadata
    namespace sql = arrow::flight::sql;
    auto sql_info_result = conn_.GetSqlInfo(
        {sql::SqlInfoOptions::FLIGHT_SQL_SERVER_VERSION,
         sql::SqlInfoOptions::FLIGHT_SQL_SERVER_ARROW_VERSION});

    auto result = conn_.ExecuteQuery(
        "SELECT GIZMOSQL_VERSION(), GIZMOSQL_EDITION(), GIZMOSQL_CURRENT_INSTANCE()");
    if (result.ok()) {
      auto& table = result->table;
      if (table->num_rows() > 0) {
        out << "     version: " << GetCellValue(table->column(0), 0, "") << "\n";
        out << "     edition: " << GetCellValue(table->column(1), 0, "") << "\n";
        out << " instance_id: " << GetCellValue(table->column(2), 0, "") << "\n";
      }
    }

    if (sql_info_result.ok()) {
      auto info_table = *sql_info_result;
      auto engine = ExtractSqlInfoString(
          info_table, sql::SqlInfoOptions::FLIGHT_SQL_SERVER_VERSION);
      auto arrow_ver = ExtractSqlInfoString(
          info_table, sql::SqlInfoOptions::FLIGHT_SQL_SERVER_ARROW_VERSION);
      if (!engine.empty()) {
        out << "      engine: " << engine << "\n";
      }
      if (!arrow_ver.empty()) {
        out << "       arrow: " << arrow_ver << "\n";
      }
    }
  }

  // --- Session ---
  out << "--- Session ---\n";
  out << "   connected: " << (conn_.IsConnected() ? "yes" : "no") << "\n";
  out << "         uri: " << BuildConnectionURI(config_) << "\n";
  out << "        host: " << config_.host << "\n";
  out << "        port: " << config_.port << "\n";
  out << "         tls: " << (config_.use_tls ? "on" : "off") << "\n";

  if (conn_.IsConnected()) {
    auto session_result = conn_.ExecuteQuery(
        "SELECT GIZMOSQL_CURRENT_SESSION(), GIZMOSQL_ROLE(), "
        "CURRENT_CATALOG(), CURRENT_SCHEMA(), GIZMOSQL_USER()");
    if (session_result.ok()) {
      auto& table = session_result->table;
      if (table->num_rows() > 0) {
        out << "    username: " << GetCellValue(table->column(4), 0, "") << "\n";
        out << "  session_id: " << GetCellValue(table->column(0), 0, "") << "\n";
        out << "        role: " << GetCellValue(table->column(1), 0, "") << "\n";
        out << "     catalog: " << GetCellValue(table->column(2), 0, "") << "\n";
        out << "      schema: " << GetCellValue(table->column(3), 0, "") << "\n";
      }
    }
  } else {
    out << "    username: " << config_.username << "\n";
  }

  // --- Settings ---
  out << "--- Settings ---\n";
  out << "        mode: " << OutputModeToString(config_.output_mode) << "\n";
  out << "     headers: " << (config_.show_headers ? "on" : "off") << "\n";
  out << "   nullvalue: \"" << config_.null_value << "\"\n";
  out << "   separator: \"" << config_.separator_col << "\" \""
      << config_.separator_row << "\"\n";
  out << "       timer: " << (config_.show_timer ? "on" : "off") << "\n";
  out << "        echo: " << (config_.echo ? "on" : "off") << "\n";
  out << "        bail: " << (config_.bail_on_error ? "on" : "off") << "\n";
  out << "     maxrows: " << config_.max_rows << "\n";
  out << "    maxwidth: " << config_.max_width << "\n";
  out << "   highlight: " << (config_.syntax_highlighting ? "on" : "off") << "\n";
  out << "       pager: " << (config_.pager_enabled ? "on" : "off")
      << " (threshold: " << config_.pager_threshold << ")\n";
}

CommandResult CommandProcessor::HandleConnect(
    const std::vector<std::string>& args) {
  if (args.size() < 2) {
    std::cerr << kErrRed
        << "Usage: .connect gizmosql://HOST:PORT[?params...]\n"
        << "       .connect HOST PORT USERNAME [PASSWORD]\n"
        << "\n"
        << "URI parameters:\n"
        << "  username=USER              Username for authentication\n"
        << "  useEncryption=true|false   Enable TLS (default: false)\n"
        << "  disableCertificateVerification=true|false\n"
        << "                             Skip TLS cert verification\n"
        << "  tlsRoots=PATH              Path to CA certificate (PEM)\n"
        << "  authType=password|external Auth type (default: password)\n"
        << kErrReset << std::endl;
    return CommandResult::ERROR;
  }

  ClientConfig new_config = config_;

  if (args[1].find("://") != std::string::npos) {
    // URI format: gizmosql://HOST:PORT[?params...]
    if (!ParseConnectionURI(args[1], new_config)) {
      return CommandResult::ERROR;
    }

    // Prompt for password if needed (non-external auth)
    if (!new_config.auth_type_external && !new_config.username.empty()) {
      if (IsTerminal()) {
        new_config.password = PromptPassword();
        new_config.password_provided = true;
      }
    }
  } else {
    // Positional format: .connect HOST PORT USERNAME [PASSWORD]
    if (args.size() < 4) {
      std::cerr << kErrRed << "Usage: .connect HOST PORT USERNAME [PASSWORD]" << kErrReset << std::endl;
      return CommandResult::ERROR;
    }

    new_config.host = args[1];
    try {
      new_config.port = std::stoi(args[2]);
    } catch (...) {
      std::cerr << kErrRed << "Error: invalid port '" << args[2] << "'" << kErrReset << std::endl;
      return CommandResult::ERROR;
    }
    new_config.username = args[3];

    if (args.size() >= 5) {
      new_config.password = args[4];
      new_config.password_provided = true;
    } else if (IsTerminal()) {
      new_config.password = PromptPassword();
      new_config.password_provided = true;
    }
  }

  auto status = conn_.Connect(new_config);
  if (!status.ok()) {
    std::cerr << kErrRed << "Error: " << status.ToString() << kErrReset << std::endl;
    return CommandResult::ERROR;
  }

  // Update config with the new connection params
  config_.host = new_config.host;
  config_.port = new_config.port;
  config_.username = new_config.username;
  config_.password = new_config.password;
  config_.password_provided = new_config.password_provided;
  config_.use_tls = new_config.use_tls;
  config_.tls_skip_verify = new_config.tls_skip_verify;
  config_.tls_roots = new_config.tls_roots;
  config_.auth_type_external = new_config.auth_type_external;

  std::cout << "Connected to " << config_.host << ":" << config_.port
            << std::endl;

  if (refresh_callback_) refresh_callback_();
  if (prompt_refresh_callback_) prompt_refresh_callback_();

  return CommandResult::OK;
}

}  // namespace gizmosql::client
