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

#ifndef SUPERSONIC_CONTRIB_STORAGE_CORE_FILE_SERIES_H_
#define SUPERSONIC_CONTRIB_STORAGE_CORE_FILE_SERIES_H_

#include <string>
#include <memory>

namespace supersonic {

// Base interface for generators of names for file series.
class FileSeries {
 public:
  virtual ~FileSeries() {}

  // Returns next name in file series.
  virtual std::string Next() = 0;

  // Returns next name in file series, but does not shift the iterator.
  virtual std::string PeepNext() = 0;

  // Returns whether there is next file in series.
  virtual bool HasNext() = 0;
};

// An enumerated series of file names, with number as a suffix.
// For example, for base name "data" it will produces "data.0", "data.1",
// "data.2" etc.
std::unique_ptr<FileSeries>
    EnumeratedFileSeries(const std::string& base_name);


// A "series" with only single file.
std::unique_ptr<FileSeries>
    SingleFileSeries(const std::string& name);

}  // namespace supersonic


#endif  // SUPERSONIC_CONTRIB_STORAGE_CORE_FILE_SERIES_H_
