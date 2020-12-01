#include "DBRegistryConfig.h"

#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>

#include <iostream>
#include <locale>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
DBRegistryConfig::DBRegistryConfig(boost::shared_ptr<SmartMet::Spine::ConfigBase> config)
{
  try
  {
    libconfig::Setting& setting = config->assert_is_group(config->get_root());

    m_name = config->get_mandatory_config_param<std::string>(setting, "name");

    // Field name restrictions checks.
    if (m_name.empty() or (std::isalpha(m_name[0], std::locale()) == 0))
    {
      Fmi::Exception exception(BCP, "Invalid table name in the config file!", nullptr);
      exception.addParameter("Table", m_name);
      exception.addParameter("Config file", config->get_file_name());
      exception.addDetail("First character of the table name must be an alphabetic character.");
      // std::cerr << exception;
      throw exception;
    }

    libconfig::Setting& fields =
        config->get_mandatory_config_param<libconfig::Setting&>(setting, "fields");
    config->assert_is_list(fields, 1);

    int numberOfFields = fields.getLength();

    for (int i = 0; i < numberOfFields; i++)
    {
      libconfig::Setting& item = config->assert_is_group(fields[i]);
      std::string fieldMethod;
      std::string fieldName;
      std::string fieldValueType;
      bool fieldIsActive = true;
      try
      {
        fieldIsActive = config->get_optional_config_param<bool>(item, "active", true);
        if (not fieldIsActive)
          continue;
        fieldName = config->get_mandatory_config_param<std::string>(item, "name");
        fieldMethod = config->get_optional_config_param<std::string>(item, "method", "");
        fieldValueType = Fmi::ascii_tolower_copy(
            config->get_optional_config_param<std::string>(item, "type", ""));
      }
      catch (const std::exception& err)
      {
        Fmi::Exception exception(
            BCP, "Error while parsing DBRegistry configuration file!", nullptr);
        exception.addParameter("Config file", config->get_file_name());
        std::ostringstream msg;
        Spine::ConfigBase::dump_setting(msg, item, 16);
        exception.addDetail(msg.str());
        throw exception;
      }

      // Field name restrictions checks.
      if (fieldName.empty() or (std::isalpha(fieldName[0], std::locale()) == 0))
      {
        Fmi::Exception exception(
            BCP, "Invalid field name '" + fieldName + "' in the configuration file!", nullptr);
        exception.addParameter("Config file", config->get_file_name());
        exception.addDetail("First character of a field name must be an alphabetic character.");
        throw exception;
      }

      // Duplicates are not allowed.
      if (m_fieldNameMap.find(fieldName) != m_fieldNameMap.end())
      {
        Fmi::Exception exception(
            BCP, "Duplicate field name '" + fieldName + "' in the configuration file!", nullptr);
        exception.addParameter("Config file", config->get_file_name());
        throw exception;
      }

      m_fieldNameMap.insert(std::pair<std::string, bool>(fieldName, fieldIsActive));
      if (not fieldMethod.empty())
        m_fieldMethodMap.insert(std::pair<NameType, NameType>(fieldName, fieldMethod));

      std::vector<std::string> valueTypeList{"int", "uint", "float", "double", "string", "ptime"};
      if (std::find(valueTypeList.begin(), valueTypeList.end(), fieldValueType) !=
          valueTypeList.end())
        m_fieldValueTypeMap.insert(std::pair<NameType, NameType>(fieldName, fieldValueType));
      else
        m_fieldValueTypeMap.insert(std::pair<NameType, NameType>(fieldName, ""));
    }

    if (m_fieldNameMap.size() == 0)
    {
      Fmi::Exception exception(
          BCP, "At least one field must be defined in the configuration file!", nullptr);
      exception.addParameter("Config file", config->get_file_name());
      throw exception;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

DBRegistryConfig::NameType DBRegistryConfig::getMethod(
    const DBRegistryConfig::NameType& fieldName) const
{
  try
  {
    FieldMethodMapType::const_iterator it = m_fieldMethodMap.find(fieldName);
    if (it != m_fieldMethodMap.end())
      return it->second;

    return "";
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

NamesAllowed::NamesAllowed(const std::shared_ptr<DBRegistryConfig> dbrConfig,
                           const bool caseSensitiveNames)
    : m_caseSensitiveNames(caseSensitiveNames)
{
  try
  {
    if (!dbrConfig)
    {
      std::cerr << "warning : Engine::::Obs::NamesAllowed class object got DBRegistryConfig "
                   "without reference.\n";
      return;
    }

    const std::shared_ptr<NameMapType> nameMap = dbrConfig->getFieldNameMap();
    if (not nameMap)
    {
      std::cerr << "warning : Engine::Observation::NamesAllowed class object use an "
                   "empty name map.\n";
      return;
    }

    m_map.clear();
    if (m_caseSensitiveNames)
    {
      m_map = *nameMap;
    }
    else
    {
      NameMapType::const_iterator it;
      for (it = nameMap->begin(); it != nameMap->end(); ++it)
      {
        std::pair<NameMapType::key_type, NameMapType::mapped_type> pair(
            Fmi::ascii_toupper_copy(it->first), it->second);
        m_map.insert(pair);
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

NamesAllowed::~NamesAllowed() {}

bool NamesAllowed::addName(const std::string& inName)
{
  try
  {
    std::string name = inName;
    if (not m_caseSensitiveNames)
      Fmi::ascii_toupper(name);

    // Is there already the name.
    std::map<std::string, bool>::const_iterator it = m_map.find(name);
    if (it == m_map.end() or !(*it).second)
      return false;

    // Is the name activated.
    if (!(*it).second)
      return false;

    m_nameList.push_back(inName);

    return true;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
