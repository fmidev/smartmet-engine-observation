#include "DBRegistry.h"
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <spine/ConfigBase.h>
#include <spine/Convenience.h>
#include <spine/Exception.h>
#include <exception>
#include <iostream>
#include <sstream>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
DBRegistry::DBRegistry() {}

DBRegistry::~DBRegistry() {}

void DBRegistry::loadConfigurations(const std::string& configFolderPath)
{
  try
  {
    boost::filesystem::path p(configFolderPath.c_str());
    if (!boost::filesystem::exists(p))
    {
      Spine::Exception exception(BCP, "Folder path does not exists!");
      exception.addParameter("Path", p.c_str());
      std::cerr << exception.getStackTrace();
      throw exception;
    }

    if (!boost::filesystem::is_directory(p))
    {
      Spine::Exception exception(BCP, "Folder path is not a directory!");
      exception.addParameter("Path", p.c_str());
      std::cerr << exception.getStackTrace();
      throw exception;
    }

    // FIXME!! Check here the folder permissions (read, execute).

    boost::filesystem::directory_iterator it;
    for (it = boost::filesystem::directory_iterator(p);
         it != boost::filesystem::directory_iterator();
         ++it)
    {
      const boost::filesystem::path entry = *it;
      const std::string fileName = entry.filename().string();

      if (boost::filesystem::is_regular_file(entry) and
          !boost::algorithm::starts_with(fileName, ".") and
          !boost::algorithm::starts_with(fileName, "#") and
          boost::algorithm::ends_with(fileName, ".conf"))
      {
        boost::shared_ptr<Spine::ConfigBase> cBase(
            new Spine::ConfigBase(entry.string(), "DBRegisty configuration"));
        try
        {
          std::shared_ptr<DBRegistryConfig> registryConfig(new DBRegistryConfig(cBase));
          m_configVector.push_back(std::move(registryConfig));
        }
        catch (const std::exception& err)
        {
          Spine::Exception exception(BCP, "DBRegistry configuration file reading failed!", nullptr);
          exception.addParameter("File", entry.string());
          std::cerr << exception.getStackTrace();
        }
      }
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

const std::shared_ptr<DBRegistryConfig> DBRegistry::dbRegistryConfig(
    const std::string& tableName) const
{
  try
  {
    DBRegistryConfigVectorType::const_iterator it = m_configVector.begin();
    for (; it != m_configVector.end(); ++it)
    {
      if ((*it)->getTableName() == tableName)
        return *it;
    }

    return std::shared_ptr<DBRegistryConfig>();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
