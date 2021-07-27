#include "VerifiableMessageQueryParams.h"
#include <macgyver/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
VerifiableMessageQueryParams::VerifiableMessageQueryParams(
    const std::shared_ptr<DBRegistryConfig> dbrConfig)
    : m_dbrConfig(dbrConfig)
{
  try
  {
    m_stationIdVector = new StationIdVectorType();
    m_namesAllowed = new NamesAllowed(dbrConfig, false);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

VerifiableMessageQueryParams::~VerifiableMessageQueryParams()
{
  m_stationIdVector->clear();
  delete m_stationIdVector;
  delete m_namesAllowed;
}

bool VerifiableMessageQueryParams::addSelectName(const std::string& selectName)
{
  try
  {
    return m_namesAllowed->addName(selectName);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void VerifiableMessageQueryParams::addStationId(const std::string& stationId)
{
  try
  {
    m_stationIdVector->push_back(stationId);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

const VerifiableMessageQueryParams::SelectNameListType*
VerifiableMessageQueryParams::getSelectNameList() const
{
  try
  {
    return m_namesAllowed->getNameList();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

VerifiableMessageQueryParams::StationIdVectorType*
VerifiableMessageQueryParams::getStationIdVector() const
{
  return m_stationIdVector;
}

VerifiableMessageQueryParams::TableNameType VerifiableMessageQueryParams::getTableName() const
{
  try
  {
    if (m_dbrConfig)
      return m_dbrConfig->getTableName();

    return "";
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

VerifiableMessageQueryParams::NameType VerifiableMessageQueryParams::getSelectNameMethod(
    const NameType& name) const
{
  try
  {
    if (m_dbrConfig)
      return m_dbrConfig->getMethod(name);

    return "";
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

bool VerifiableMessageQueryParams::isRestriction(int id) const
{
  try
  {
    const auto it = m_restrictionMap.find(id);
    if (it != m_restrictionMap.end())
      return (*it).second;
    return false;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void VerifiableMessageQueryParams::setReturnOnlyLatest()
{
  try
  {
    const auto it = m_restrictionMap.find(Restriction::RETURN_ONLY_LATEST);
    if (it == m_restrictionMap.end())
      m_restrictionMap.insert(std::pair<int, bool>(Restriction::RETURN_ONLY_LATEST, true));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
