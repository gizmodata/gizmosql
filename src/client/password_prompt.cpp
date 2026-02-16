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

#include "password_prompt.hpp"

#include <iostream>
#include <termios.h>
#include <unistd.h>

namespace gizmosql::client {

bool IsTerminal() {
  return isatty(STDIN_FILENO) != 0;
}

std::string PromptPassword(const std::string& prompt) {
  std::cerr << prompt;

  struct termios old_term, new_term;
  tcgetattr(STDIN_FILENO, &old_term);
  new_term = old_term;
  new_term.c_lflag &= ~ECHO;
  tcsetattr(STDIN_FILENO, TCSANOW, &new_term);

  std::string password;
  std::getline(std::cin, password);

  tcsetattr(STDIN_FILENO, TCSANOW, &old_term);
  std::cerr << std::endl;

  return password;
}

}  // namespace gizmosql::client
