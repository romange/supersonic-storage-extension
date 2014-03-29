// Copyright 2012 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// The file provides simple path functionalities.
// TODO(wzoltak): tests

#ifndef SUPERSONIC_CONTRIB_STORAGE_UTIL_PATH_UTIL_H_
#define SUPERSONIC_CONTRIB_STORAGE_UTIL_PATH_UTIL_H_

#include <sys/types.h>
#include <string>

namespace supersonic {

// Wrapper class for system functions which handle basic path/directory
// operations.
class PathUtil {
 public:
  // Creates directory returning true iff successful.
  static bool MkDir(const std::string& path, mode_t mode, bool with_parents);

  // Returns true if directory exists. Returns false if directory does not
  // exist or if an error is encountered.
  static bool ExistsDir(const std::string& path);

  // Removes directory along with its contents. Returns false if directory
  // does not exist or if an error is encountered, true if successful.
  static bool RecursiveDelete(const std::string& path);
};

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_UTIL_PATH_UTIL_H_
