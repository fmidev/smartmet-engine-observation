#pragma once

#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct SpatiaLiteOptions
{
  std::string synchronous = "NORMAL";          // OFF | NORMAL | FULL | EXTRA
  std::string journal_mode = "WAL";            // DELETE | TRUNCATE | PERSIST | MEMORY | WAL | OFFD
  std::string threading_mode = "MULTITHREAD";  // MULTITHREAD | SERIALIZED
  std::string auto_vacuum = "NONE";            // NONE | FULL | INCREMENTAL
  std::string temp_store = "DEFAULT";          // DEFAULT | FILE | MEMORY
  std::size_t mmap_size = 0;
  bool memstatus = false;
  bool shared_cache = false;
  bool read_uncommitted = false;
  int threads = 0;                // 0 = number of helper threads
  int timeout = 30000;            // milliseconds
  long cache_size = 0;            // positive = bytes, negative = nro of pages, zero = use defaults
  int wal_autocheckpoint = 1000;  // 0=disables, 1000 is sqlite default
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
