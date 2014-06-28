// Copyright 2014 Google Inc.  All Rights Reserved
// Author: Wojtek Żółtak (wojciech.zoltak@gmail.com)
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

#include "supersonic/contrib/storage/core/file_series.h"

#include <memory>
#include <sstream>

#include "supersonic/utils/macros.h"

namespace supersonic {
namespace {

class EnumeratedFileSeries : public FileSeries {
 public:
  explicit EnumeratedFileSeries(const std::string& name)
      : name_(name),
        chunk_(0) {}

  virtual ~EnumeratedFileSeries() {}

  std::string NextFileName() {
    std::stringstream ss;
    ss << name_ << "." << chunk_++;
    return ss.str();
  }

 private:
  const std::string name_;
  size_t chunk_;
  DISALLOW_COPY_AND_ASSIGN(EnumeratedFileSeries);
};

}  // namespace


std::unique_ptr<FileSeries>
    EnumeratedFileSeries(const std::string& base_name) {
  return std::unique_ptr<FileSeries>(
      new class EnumeratedFileSeries(base_name));
}


}  // namespace supersonic
