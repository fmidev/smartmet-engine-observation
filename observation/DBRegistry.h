#pragma once

#include "DBRegistryConfig.h"
#include <memory>
#include <string>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class DBRegistry
{
 public:
  typedef std::vector<std::shared_ptr<DBRegistryConfig> > DBRegistryConfigVectorType;
  explicit DBRegistry();

  ~DBRegistry();

  DBRegistry(const DBRegistry& other) = delete;
  DBRegistry& operator=(const DBRegistry& other) = delete;

  void loadConfigurations(const std::string& configFolderPath);

  /**
   *  @brief Pointer to the configuration that match with the given tableName.
   *  @return Pointer to the configuration will be returend if there is a match and otherwise NULL.
   */
  std::shared_ptr<DBRegistryConfig> dbRegistryConfig(const std::string& tableName) const;

 private:
  std::string m_configFolderPath;
  DBRegistryConfigVectorType m_configVector;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

/**

@page OBSENGINE_DB_REGISTRY Database registry

- Configuration file name suffix must be ".conf" and files with prefix "." or "#"
  are ignored.

*/
