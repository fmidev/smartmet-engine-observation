#ifndef DB_REGISTRY_H
#define DB_REGISTRY_H

#include <string>
#include <vector>
#include <memory>
#include "DBRegistryConfig.h"

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

  void loadConfigurations(const std::string& configFolderPath);

  /**
   *  @brief Pointer to the configuration that match with the given tableName.
   *  @return Pointer to the configuration will be returend if there is a match and otherwise NULL.
   */
  const std::shared_ptr<DBRegistryConfig> dbRegistryConfig(const std::string& tableName) const;

 private:
  DBRegistry(const DBRegistry& other);
  DBRegistry& operator=(const DBRegistry& other);

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

#endif  // DB_REGISTRY_H
