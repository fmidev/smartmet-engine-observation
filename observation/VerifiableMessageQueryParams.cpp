#include "VerifiableMessageQueryParams.h"
#include <spine/Exception.h>

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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

VerifiableMessageQueryParams::~VerifiableMessageQueryParams()
{
  try
  {
    m_stationIdVector->clear();
    delete m_stationIdVector;
    delete m_namesAllowed;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

bool VerifiableMessageQueryParams::addSelectName(const std::string& selectName)
{
  try
  {
    return m_namesAllowed->addName(selectName);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

bool VerifiableMessageQueryParams::isRestriction(int id) const
{
  try
  {
    std::unordered_map<int, bool>::const_iterator it = m_restrictionMap.find(id);
    if (it != m_restrictionMap.end())
      return (*it).second;
    else
      return false;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void VerifiableMessageQueryParams::setReturnOnlyLatest()
{
  try
  {
    std::unordered_map<int, bool>::const_iterator it =
        m_restrictionMap.find(Restriction::RETURN_ONLY_LATEST);
    if (it == m_restrictionMap.end())
      m_restrictionMap.insert(std::pair<int, bool>(Restriction::RETURN_ONLY_LATEST, true));
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
