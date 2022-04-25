#include "StationtypeConfig.h"
#include <boost/algorithm/string.hpp>
#include <boost/make_shared.hpp>
#include <fmt/format.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <iostream>
#include <sstream>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
  static StationtypeConfig::ProducerIdSetType empty_producer_id_set;

StationtypeConfig::StationtypeConfig() = default;

StationtypeConfig::~StationtypeConfig() = default;

void StationtypeConfig::addStationtype(const StationtypeType& stationtype,
                                       const GroupCodeVectorType& stationgroupVector)
{
  try
  {
    const StationtypeType stationtypeLower = Fmi::ascii_tolower_copy(stationtype);

    if (stationtypeLower.empty())
    {
      throw Fmi::Exception(BCP,
                           "Engine::Observation::StationtypeConfig::addStationtype : Empty "
                           "stationtype name found.");
    }

    auto it = m_stationtypeMap.find(stationtypeLower);
    if (it != m_stationtypeMap.end())
    {
      throw Fmi::Exception(BCP,
                           "Engine::Observation::StationtypeConfig::addStationtype : "
                           "Duplicate stationtype configuration '" +
                               stationtype + "'.");
    }

    if (stationgroupVector.empty())
    {
      throw Fmi::Exception(BCP,
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
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::string StationtypeConfig::getDatabaseTableNameByStationtype(const StationtypeType& stationtype) const
{
  try
  {
    const StationtypeType stationtypeLower = Fmi::ascii_tolower_copy(stationtype);
    const auto it = m_stDatabaseTableNameMap.find(stationtypeLower);
    if (it == m_stDatabaseTableNameMap.end())
	  return "";

    return it->second;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::shared_ptr<const StationtypeConfig::GroupCodeSetType>
StationtypeConfig::getGroupCodeSetByStationtype(const StationtypeType& stationtype) const
{
  try
  {
    const StationtypeType stationtypeLower = Fmi::ascii_tolower_copy(stationtype);
    const auto it = m_stationtypeMap.find(stationtypeLower);
    if (it != m_stationtypeMap.end())
      return std::make_shared<GroupCodeSetType>(it->second);

    throw Fmi::Exception(BCP, "Invalid parameter value!")
        .addDetail(fmt::format("Stationtype '{}' not found.", stationtype));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

const StationtypeConfig::ProducerIdSetType&
StationtypeConfig::getProducerIdSetByStationtype(const StationtypeType& stationtype) const
{
  try
  {
    const StationtypeType stationtypeLower = Fmi::ascii_tolower_copy(stationtype);
    const auto it = m_stProducerIdSetMap.find(stationtypeLower);
    if (it != m_stProducerIdSetMap.end())
      return it->second;
	
	return empty_producer_id_set;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

StationtypeConfig::UseCommonQueryMethodType StationtypeConfig::getUseCommonQueryMethod(
    const StationtypeType& stationtype) const
{
  try
  {
    const StationtypeType stationtypeLower = Fmi::ascii_tolower_copy(stationtype);
    const auto it = m_stUseCommonQueryMethodMap.find(stationtypeLower);
    if (it != m_stUseCommonQueryMethodMap.end())
      return it->second;
    return false;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void StationtypeConfig::setDatabaseTableName(const StationtypeType& stationtype,
                                             const DatabaseTableNameType& databaseTableName)
{
  try
  {
    // Checking that the stationtype has added.
    const StationtypeType stationtypeLower = Fmi::ascii_tolower_copy(stationtype);
    const auto stGroupCodeSetMapIt = m_stationtypeMap.find(stationtypeLower);
    if (stGroupCodeSetMapIt == m_stationtypeMap.end())
      throw Fmi::Exception(BCP, "Invalid parameter value!")
          .addDetail(fmt::format(
              "Stationtype '{}' not found. Add first the stationtype into the class object.",
              stationtype));

    // Checking that there is not already added a database table name.
    const auto stDatabaseTableNameMapIt = m_stDatabaseTableNameMap.find(stationtypeLower);

    if (stDatabaseTableNameMapIt != m_stDatabaseTableNameMap.end())
      throw Fmi::Exception(BCP, "Invalid parameter value!")
          .addDetail(fmt::format(
              "There is already added a database table name '{}' for the stationtype '{}'. Table "
              "name '{}' is not added.",
              stDatabaseTableNameMapIt->second,
              stationtype,
              databaseTableName));

    const DatabaseTableNameType tablenameLower = Fmi::ascii_tolower_copy(databaseTableName);

    // We do not want to store empty values.
    if (tablenameLower.empty())
      throw Fmi::Exception(BCP, "Invalid parameter value!")
          .addDetail(fmt::format("The database table name is empty for the stationtype '{}'.",
                                 stationtype));

    m_stDatabaseTableNameMap.emplace(std::make_pair(stationtypeLower, tablenameLower));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
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
      throw Fmi::Exception(BCP, "Invalid parameter value!")
          .addDetail(fmt::format(
              "Stationtype '{}' not found. Add first the stationtype into the class object.",
              stationtype));

    m_stUseCommonQueryMethodMap.emplace(std::make_pair(stationtypeLower, value));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
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
    const auto stGroupCodeSetMapIt = m_stationtypeMap.find(stationtypeLower);

    if (stGroupCodeSetMapIt == m_stationtypeMap.end())
      throw Fmi::Exception(BCP, "Invalid parameter value!")
          .addDetail(fmt::format(
              "Stationtype '{}' not found. Add first the stationtype into the class object.",
              stationtype));

    auto stProducerIdSetMapIt = m_stProducerIdSetMap.find(stationtypeLower);

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
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

bool StationtypeConfig::hasProducerIds(const StationtypeType& stationtype) const
{
  try
  {
    const StationtypeType stationtypeLower = Fmi::ascii_tolower_copy(stationtype);
    const auto it = m_stProducerIdSetMap.find(stationtypeLower);
    return (it != m_stProducerIdSetMap.end());
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

bool StationtypeConfig::hasGroupCodes(const StationtypeType& stationtype) const
{
  try
  {
    const StationtypeType stationtypeLower = Fmi::ascii_tolower_copy(stationtype);
    const auto it = m_stationtypeMap.find(stationtypeLower);
    return (it != m_stationtypeMap.end());
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
