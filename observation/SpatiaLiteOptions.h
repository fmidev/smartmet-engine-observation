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
  std::size_t mmap_size = 0;
  bool memstatus = false;
  bool shared_cache = false;
  int timeout = 30000;  // milliseconds
  long cache_size = 0;  // positive = bytes, negative = nro of pages, zero = use defaults
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
