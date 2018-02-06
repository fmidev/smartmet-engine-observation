#include "StationtypeConfig.h"

#include <boost/algorithm/string.hpp>
#include <macgyver/StringConversion.h>
#include <spine/Exception.h>
#include <iostream>
#include <sstream>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
StationtypeConfig::StationtypeConfig() {}

StationtypeConfig::~StationtypeConfig() {}

void StationtypeConfig::addStationtype(const StationtypeType& stationtype,
                                       const GroupCodeVectorType& stationgroupVector)
{
  try
  {
    const StationtypeType stationtypeLower = Fmi::ascii_tolower_copy(stationtype);

    if (stationtypeLower.empty())
    {
      throw Spine::Exception(BCP,
                             "Engine::Observation::StationtypeConfig::addStationtype : Empty "
                             "stationtype name found.");
    }

    STGroupCodeSetMapType::iterator it = m_stationtypeMap.find(stationtypeLower);
    if (it != m_stationtypeMap.end())
    {
      throw Spine::Exception(BCP,
                             "Engine::Observation::StationtypeConfig::addStationtype : "
                             "Duplicate stationtype configuration '" +
                                 stationtype + "'.");
    }

    if (stationgroupVector.empty())
    {
      throw Spine::Exception(BCP,
                             "Engine::Observation::StationtypeConfig::addStationtype : Empty "
                             "group code array found for '" +
                                 stationtype + "' stationtype");
    }

    // We do not check the group code values, so e.g. zero length codes are allowed.

    m_stationtypeMap.emplace(std::make_pair(stationtypeLower, GroupCodeSetType()));
    it = m_stationtypeMap.find(stationtypeLower);
    it->second.insert(stationgroupVector.begin(), stationgroupVector.end());
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

std::shared_ptr<const StationtypeConfig::DatabaseTableNameType>
StationtypeConfig::getDatabaseTableNameByStationtype(const StationtypeType& stationtype) const
{
  try
  {
    const StationtypeType stationtypeLower = Fmi::ascii_tolower_copy(stationtype);
    STDatabaseTableNameMapType::const_iterator it = m_stDatabaseTableNameMap.find(stationtypeLower);
    if (it == m_stDatabaseTableNameMap.end())
    {
      std::ostringstream msg;
      msg << "Database table name for the stationtype '" << stationtype << "' not found.";

      Spine::Exception exception(BCP, "Invalid parameter value!");
      // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
      exception.addDetail(msg.str());
      throw exception;
    }

    return std::make_shared<DatabaseTableNameType>(it->second);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

std::shared_ptr<const StationtypeConfig::GroupCodeSetType>
StationtypeConfig::getGroupCodeSetByStationtype(const StationtypeType& stationtype) const
{
  try
  {
    const StationtypeType stationtypeLower = Fmi::ascii_tolower_copy(stationtype);
    STGroupCodeSetMapType::const_iterator it = m_stationtypeMap.find(stationtypeLower);
    if (it != m_stationtypeMap.end())
    {
      return std::make_shared<GroupCodeSetType>(it->second);
    }
    else
    {
      std::ostringstream msg;
      msg << "Stationtype '" << stationtype << "' not found.";

      Spine::Exception exception(BCP, "Invalid parameter value!");
      // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
      exception.addDetail(msg.str());
      throw exception;
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

std::shared_ptr<const StationtypeConfig::ProducerIdSetType>
StationtypeConfig::getProducerIdSetByStationtype(const StationtypeType& stationtype) const
{
  try
  {
    const StationtypeType stationtypeLower = Fmi::ascii_tolower_copy(stationtype);
    STProducerIdSetMapType::const_iterator it = m_stProducerIdSetMap.find(stationtypeLower);
    if (it != m_stProducerIdSetMap.end())
    {
      return std::make_shared<ProducerIdSetType>(it->second);
    }
    else
    {
      std::ostringstream msg;
      msg << "Producer id list not found for Stationtype '" << stationtype << "'.";
      Spine::Exception exception(BCP, "Invalid parameter value!");
      // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
      exception.addDetail(msg.str());
      throw exception;
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

StationtypeConfig::UseCommonQueryMethodType StationtypeConfig::getUseCommonQueryMethod(
    const StationtypeType& stationtype) const
{
  try
  {
    const StationtypeType stationtypeLower = Fmi::ascii_tolower_copy(stationtype);
    STUseCommonQueryMethodMapType::const_iterator it =
        m_stUseCommonQueryMethodMap.find(stationtypeLower);
    if (it != m_stUseCommonQueryMethodMap.end())
      return it->second;
    else
      return false;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void StationtypeConfig::setDatabaseTableName(const StationtypeType& stationtype,
                                             const DatabaseTableNameType& databaseTableName)
{
  try
  {
    // Checking that the stationtype has added.
    const StationtypeType stationtypeLower = Fmi::ascii_tolower_copy(stationtype);
    STGroupCodeSetMapType::const_iterator stGroupCodeSetMapIt =
        m_stationtypeMap.find(stationtypeLower);
    if (stGroupCodeSetMapIt == m_stationtypeMap.end())
    {
      std::ostringstream msg;
      msg << "Stationtype '" << stationtype
          << "' not found. Add first the stationtype into the class object.";

      Spine::Exception exception(BCP, "Invalid parameter value!");
      // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
      exception.addDetail(msg.str());
      throw exception;
    }

    // Checking that there is not already added a database table name.
    STDatabaseTableNameMapType::const_iterator stDatabaseTableNameMapIt =
        m_stDatabaseTableNameMap.find(stationtypeLower);
    if (stDatabaseTableNameMapIt != m_stDatabaseTableNameMap.end())
    {
      std::ostringstream msg;
      msg << "There is already added a database table name '" << stDatabaseTableNameMapIt->second
          << "'  for the stationtype '" << stationtype << "'. Table name '" << databaseTableName
          << "' is not added.";

      Spine::Exception exception(BCP, "Invalid parameter value!");
      // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
      exception.addDetail(msg.str());
      throw exception;
    }

    const DatabaseTableNameType tablenameLower = Fmi::ascii_tolower_copy(databaseTableName);

    // We do not want to store empty values.
    if (tablenameLower.empty())
    {
      std::ostringstream msg;
      msg << "The database table name is empty for the stationtype '" << stationtype << "'.";

      Spine::Exception exception(BCP, "Invalid parameter value!");
      // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
      exception.addDetail(msg.str());
      throw exception;
    }

    m_stDatabaseTableNameMap.emplace(std::make_pair(stationtypeLower, tablenameLower));
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void StationtypeConfig::setUseCommonQueryMethod(const StationtypeType& stationtype,
                                                const UseCommonQueryMethodType& value)
{
  try
  {
    // Checking that the stationtype has added.
    const StationtypeType stationtypeLower = Fmi::ascii_tolower_copy(stationtype);
    STGroupCodeSetMapType::const_iterator stGroupCodeSetMapIt =
        m_stationtypeMap.find(stationtypeLower);
    if (stGroupCodeSetMapIt == m_stationtypeMap.end())
    {
      std::ostringstream msg;
      msg << "Stationtype '" << stationtype
          << "' not found. Add first the stationtype into the class object.";

      Spine::Exception exception(BCP, "Invalid parameter value!");
      // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
      exception.addDetail(msg.str());
      throw exception;
    }

    m_stUseCommonQueryMethodMap.emplace(std::make_pair(stationtypeLower, value));
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void StationtypeConfig::setProducerIds(const StationtypeType& stationtype,
                                       const ProducerIdVectorType& producerIdVector)
{
  try
  {
    if (producerIdVector.empty())
      return;

    // Checking that the stationtype has added.
    const StationtypeType stationtypeLower = Fmi::ascii_tolower_copy(stationtype);
    STGroupCodeSetMapType::const_iterator stGroupCodeSetMapIt =
        m_stationtypeMap.find(stationtypeLower);
    if (stGroupCodeSetMapIt == m_stationtypeMap.end())
    {
      std::ostringstream msg;
      msg << "Stationtype '" << stationtype
          << "' not found. Add first the stationtype into the class object.";

      Spine::Exception exception(BCP, "Invalid parameter value!");
      // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
      exception.addDetail(msg.str());
      throw exception;
    }

    STProducerIdSetMapType::iterator stProducerIdSetMapIt =
        m_stProducerIdSetMap.find(stationtypeLower);

    // Create a producer set for the station type or clear the old values.
    if (stProducerIdSetMapIt == m_stProducerIdSetMap.end())
    {
      m_stProducerIdSetMap.emplace(std::make_pair(stationtypeLower, ProducerIdSetType()));
      stProducerIdSetMapIt = m_stProducerIdSetMap.find(stationtypeLower);
    }
    else
      stProducerIdSetMapIt->second.clear();

    // Insert the producer values.
    stProducerIdSetMapIt->second.insert(producerIdVector.begin(), producerIdVector.end());
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
