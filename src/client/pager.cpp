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

#include "pager.hpp"

#include <algorithm>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#ifdef _WIN32
#define NOMINMAX
#include <conio.h>
#include <windows.h>
#else
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>
#endif

namespace gizmosql::client {

int Pager::GetTerminalHeight() {
#ifdef _WIN32
  CONSOLE_SCREEN_BUFFER_INFO csbi;
  if (GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &csbi)) {
    int height = csbi.srWindow.Bottom - csbi.srWindow.Top + 1;
    if (height > 0) return height;
  }
#elif defined(__unix__) || defined(__APPLE__)
  struct winsize ws;
  if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0 && ws.ws_row > 0) {
    return ws.ws_row;
  }
#endif
  return 24;  // default
}

namespace {

std::vector<std::string> SplitLines(const std::string& content) {
  std::vector<std::string> lines;
  std::istringstream stream(content);
  std::string line;
  while (std::getline(stream, line)) {
    lines.push_back(line);
  }
  return lines;
}

#ifndef _WIN32

struct RawModeGuard {
  struct termios orig_;
  bool active_ = false;

  bool Enter() {
    if (!isatty(STDIN_FILENO)) return false;
    if (tcgetattr(STDIN_FILENO, &orig_) != 0) return false;
    struct termios raw = orig_;
    raw.c_lflag &= ~(ICANON | ECHO);
    raw.c_cc[VMIN] = 1;
    raw.c_cc[VTIME] = 0;
    if (tcsetattr(STDIN_FILENO, TCSANOW, &raw) != 0) return false;
    active_ = true;
    return true;
  }

  ~RawModeGuard() {
    if (active_) {
      tcsetattr(STDIN_FILENO, TCSANOW, &orig_);
    }
  }
};

// Read a keypress, handling escape sequences for special keys.
// Returns: 'q' for quit, 'j'/'k' for line scroll, 'n'/'p' for page down/up,
//          'g' for home, 'G' for end.
char ReadKey() {
  char c;
  if (read(STDIN_FILENO, &c, 1) != 1) return 'q';

  if (c == 'q' || c == 'Q') return 'q';
  if (c == 'j') return 'j';
  if (c == 'k') return 'k';
  if (c == ' ') return 'n';  // Space = page down
  if (c == 'g') return 'g';
  if (c == 'G') return 'G';

  // Escape sequence
  if (c == '\033') {
    char seq[3];
    if (read(STDIN_FILENO, &seq[0], 1) != 1) return 'q';  // Bare Escape = quit
    if (seq[0] != '[') return 'q';
    if (read(STDIN_FILENO, &seq[1], 1) != 1) return 'q';

    switch (seq[1]) {
      case 'A': return 'k';  // Up arrow
      case 'B': return 'j';  // Down arrow
      case '5':              // Page Up: ESC[5~
        read(STDIN_FILENO, &seq[2], 1);
        return 'p';
      case '6':              // Page Down: ESC[6~
        read(STDIN_FILENO, &seq[2], 1);
        return 'n';
      case 'H': return 'g';  // Home
      case 'F': return 'G';  // End
      default: return 0;     // Unknown, ignore
    }
  }

  return 0;  // Unknown key, ignore
}

#else  // _WIN32

char ReadKey() {
  int c = _getch();
  if (c == 'q' || c == 'Q') return 'q';
  if (c == 'j') return 'j';
  if (c == 'k') return 'k';
  if (c == ' ') return 'n';
  if (c == 'g') return 'g';
  if (c == 'G') return 'G';
  if (c == 27) return 'q';  // Escape
  if (c == 0 || c == 0xE0) {
    int ext = _getch();
    switch (ext) {
      case 72: return 'k';  // Up
      case 80: return 'j';  // Down
      case 73: return 'p';  // Page Up
      case 81: return 'n';  // Page Down
      case 71: return 'g';  // Home
      case 79: return 'G';  // End
    }
  }
  return 0;
}

#endif

void ClearLine() {
  std::cout << "\r\033[K" << std::flush;
}

}  // namespace

void Pager::Display(const std::string& content) {
#ifndef _WIN32
  if (!isatty(STDOUT_FILENO)) {
    std::cout << content;
    return;
  }
#endif

  auto lines = SplitLines(content);
  if (lines.empty()) return;

  int term_height = GetTerminalHeight();
  int page_height = term_height - 1;  // Reserve 1 line for status bar
  if (page_height < 1) page_height = 1;

  int total_lines = static_cast<int>(lines.size());

  // If content fits on screen, just print it
  if (total_lines <= page_height) {
    std::cout << content;
    return;
  }

#ifndef _WIN32
  RawModeGuard raw_mode;
  if (!raw_mode.Enter()) {
    // Can't enter raw mode — just print everything
    std::cout << content;
    return;
  }
#endif

  int scroll_pos = 0;  // Index of the first visible line
  int max_scroll = std::max(0, total_lines - page_height);

  // Switch to alternate screen buffer so the terminal's own scroll buffer
  // doesn't interfere with our pager (macOS Terminal intercepts Fn+Up/Down
  // for its scroll buffer otherwise).
  std::cout << "\033[?1049h" << std::flush;

  auto render_page = [&]() {
    // Clear screen and move cursor to top
    std::cout << "\033[2J\033[H";

    int end = std::min(scroll_pos + page_height, total_lines);
    for (int i = scroll_pos; i < end; ++i) {
      std::cout << lines[i] << "\n";
    }

    // Status bar (inverted colors)
    int page = (scroll_pos / page_height) + 1;
    int total_pages = (total_lines + page_height - 1) / page_height;
    float pct = total_lines > 0
                    ? 100.0f * (scroll_pos + page_height) / total_lines
                    : 100.0f;
    if (pct > 100.0f) pct = 100.0f;

    std::cout << "\033[7m"  // Reverse video
              << " -- Page " << page << "/" << total_pages
              << " (" << static_cast<int>(pct) << "%)"
              << " -- q:quit  PgUp/PgDn:navigate  j/k:scroll"
              << " -- "
              << "\033[0m" << std::flush;
  };

  render_page();

  while (true) {
    char key = ReadKey();
    if (key == 'q') break;

    int old_pos = scroll_pos;
    switch (key) {
      case 'n':  // Page down
        scroll_pos = std::min(scroll_pos + page_height, max_scroll);
        break;
      case 'p':  // Page up
        scroll_pos = std::max(scroll_pos - page_height, 0);
        break;
      case 'j':  // Line down
        scroll_pos = std::min(scroll_pos + 1, max_scroll);
        break;
      case 'k':  // Line up
        scroll_pos = std::max(scroll_pos - 1, 0);
        break;
      case 'g':  // Home
        scroll_pos = 0;
        break;
      case 'G':  // End
        scroll_pos = max_scroll;
        break;
      default:
        continue;
    }

    if (scroll_pos != old_pos) {
      render_page();
    }
  }

  // Leave alternate screen buffer — restores the original terminal content
  std::cout << "\033[?1049l";

  // Re-print the content that was visible when user quit
  int end = std::min(scroll_pos + page_height, total_lines);
  for (int i = scroll_pos; i < end; ++i) {
    std::cout << lines[i] << "\n";
  }
  std::cout << std::flush;
}

}  // namespace gizmosql::client
