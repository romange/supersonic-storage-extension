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
//
//
// Utils for dealing with directories.

#include "supersonic/contrib/storage/util/path_util.h"

#include <dirent.h>
#include <glog/logging.h>
#include <libgen.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string>

#include "supersonic/utils/file.h"

namespace supersonic {

bool PathUtil::MkDir(const std::string& path, mode_t mode, bool with_parents) {
  int c_path_buffer_length = strlen(path.c_str()) + 1;
  std::unique_ptr<char> c_path(new char[c_path_buffer_length]);
  snprintf(c_path.get(), c_path_buffer_length, "%s", path.c_str());

  char* parent_name = dirname(c_path.get());
  if (!ExistsDir(parent_name)) {
    if (!with_parents ||
        (with_parents && !MkDir(parent_name, mode, with_parents))) {
      return false;
    }
  }
  if (mkdir(path.c_str(), mode) != 0) {
    LOG(WARNING) << "Can't create directory " << path
                 << " because mkdir() failed "
                 << "(errno = " << strerror(errno) << ").";
    return false;
  }

  return true;
}

bool PathUtil::ExistsDir(const std::string& path) {
  struct stat status;
  if (stat(path.c_str(), &status) != 0) {
    if (errno != ENOENT) {
      LOG(WARNING) << "Can't check existence of " << path
                   << " because stat() failed "
                   << "(errno = " << strerror(errno) << ").";
    }
    return false;
  } else {
    return S_ISDIR(status.st_mode);
  }
}

bool PathUtil::RecursiveDelete(const std::string& path) {
  // TODO(wzoltak): monstrous function, slice it?
  DIR* directory = opendir(path.c_str());
  if (directory == nullptr) {
    LOG(WARNING) << "Can't remove directory " << path
                 << " because opendir() failed "
                 << "(errno = " << strerror(errno) << ").";
    return false;
  }

  struct dirent* entry;
  struct stat entry_status;

  while (true) {
    // Read entry from directory.
    errno = 0;
    entry = readdir(directory);
    if (entry == nullptr) {
      if (errno == 0) {
        // No more entries.
        break;
      } else {
        LOG(WARNING) << "Can't remove directory " << path
                     << " because readdir() failed "
                     << "(errno = " << strerror(errno) << ").";
        return false;
      }
    }
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    std::string entry_path = File::JoinPath(path, entry->d_name);

    // Check entry type (directory/file/something else) using sys/stat.h.
    if (stat(entry_path.c_str(), &entry_status) != 0) {
      LOG(WARNING) << "Can't remove directory " << path
                   << " because stat() failed "
                   << "(errno = " << strerror(errno) << ").";
      return false;
    }

    if (S_ISDIR(entry_status.st_mode)) {
      // Delete child directory.
      printf("%s \n", entry->d_name);
      if (!RecursiveDelete(entry_path)) {
        return false;
      }
    } else if (S_ISREG(entry_status.st_mode)) {
      // Delete child regular file.
      if (unlink(entry_path.c_str()) != 0) {
        LOG(WARNING) << "Can't remove regular file " << entry_path
                     << " because unlink() failed "
                     << "(errno = " << strerror(errno) << ").";
        return false;
      }
    } else {
      // Fail on other entry types.
      LOG(WARNING) << "Can't remove directory " << path
                   << " because entry " << entry->d_name
                   << " is neither a directory, nor a regular file.";
      return false;
    }
  }

  // Close and remove the directory itself.
  if (closedir(directory) != 0) {
    LOG(WARNING) << "Can't remove directory " << path
                 << " because closedir() failed "
                 << "(errno = " << strerror(errno) << ").";
    return false;
  }
  if (rmdir(path.c_str()) != 0) {
    LOG(WARNING) << "Can't remove directory " << path
                     << " because rmdir() failed "
                     << "(errno = " << strerror(errno) << ").";
    return false;
  }

  return true;
}

}  // namespace supersonic
