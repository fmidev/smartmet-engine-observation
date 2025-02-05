#include "DBRegistry.h"
#include <boost/algorithm/string.hpp>
#include <macgyver/Exception.h>
#include <spine/ConfigBase.h>
#include <spine/Convenience.h>
#include <exception>
#include <filesystem>
#include <iostream>
#include <sstream>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
DBRegistry::DBRegistry() = default;

DBRegistry::~DBRegistry() = default;

void DBRegistry::loadConfigurations(const std::string& configFolderPath)
{
  try
  {
    std::filesystem::path p(configFolderPath.c_str());
    if (!std::filesystem::exists(p))
    {
      Fmi::Exception exception(BCP, "Folder path does not exists!");
      exception.addParameter("Path", p.c_str());
      std::cerr << exception.getStackTrace();
      throw exception;
    }

    if (!std::filesystem::is_directory(p))
    {
      Fmi::Exception exception(BCP, "Folder path is not a directory!");
      exception.addParameter("Path", p.c_str());
      std::cerr << exception.getStackTrace();
      throw exception;
    }

    // FIXME!! Check here the folder permissions (read, execute).

    std::filesystem::directory_iterator it;
    for (it = std::filesystem::directory_iterator(p); it != std::filesystem::directory_iterator();
         ++it)
    {
      const std::filesystem::path entry = *it;
      const std::string fileName = entry.filename().string();

      if (std::filesystem::is_regular_file(entry) and
          !boost::algorithm::starts_with(fileName, ".") and
          !boost::algorithm::starts_with(fileName, "#") and
          boost::algorithm::ends_with(fileName, ".conf"))
      {
        std::shared_ptr<Spine::ConfigBase> cBase(
            new Spine::ConfigBase(entry.string(), "DBRegisty configuration"));
        try
        {
          std::shared_ptr<DBRegistryConfig> registryConfig(new DBRegistryConfig(cBase));
          m_configVector.push_back(std::move(registryConfig));
        }
        catch (const std::exception& err)
        {
          Fmi::Exception exception(BCP, "DBRegistry configuration file reading failed!", nullptr);
          exception.addParameter("File", entry.string());
          std::cerr << exception.getStackTrace();
        }
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::shared_ptr<DBRegistryConfig> DBRegistry::dbRegistryConfig(const std::string& tableName) const
{
  try
  {
    for (const auto& config : m_configVector)
    {
      if (config->getTableName() == tableName)
        return config;
    }

    return {};
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
