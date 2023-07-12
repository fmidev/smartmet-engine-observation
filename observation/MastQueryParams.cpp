#include "MastQueryParams.h"
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
MastQueryParams::MastQueryParams(const std::shared_ptr<DBRegistryConfig>& dbrConfig)
{
  try
  {
    if (!dbrConfig)
    {
      Fmi::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail("Database registry configuration is not set.");
      throw exception;
    }

    m_dbrConfig.push_back(dbrConfig);
    m_conformanceClass = std::make_shared<ExtendedStandardFilter>();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

MastQueryParams::~MastQueryParams() = default;

void MastQueryParams::addJoinOnConfig(const std::shared_ptr<DBRegistryConfig>& dbrConfigJoinOn,
                                      const NameType& joinOnField,
                                      int typeOfJoin)
{
  try
  {
    std::list<NameType> joinOnFields;
    joinOnFields.push_back(joinOnField);
    addJoinOnConfig(dbrConfigJoinOn, joinOnFields, typeOfJoin);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void MastQueryParams::addJoinOnConfig(const std::shared_ptr<DBRegistryConfig>& dbrConfigJoinOn,
                                      const std::list<NameType>& joinOnFields,
                                      int typeOfJoin)
{
  try
  {
    if (!dbrConfigJoinOn)
    {
      Fmi::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail("Database registry configuration is not set.");
      throw exception;
    }
    if (joinOnFields.empty())
    {
      Fmi::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(ObsEngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail("Join fields not set.");
      throw exception;
    }

    for (const auto& joinOnField : joinOnFields)
    {
      // Try to find the joinOnField from the first registry config. Fail if not found.
      const std::string joinOnFieldUpperCase = Fmi::ascii_toupper_copy(joinOnField);
      const std::shared_ptr<DBRegistryConfig::FieldNameMapType> map =
          m_dbrConfig.at(0)->getFieldNameMap();
      const auto it = map->find(joinOnFieldUpperCase);

      const std::shared_ptr<DBRegistryConfig::FieldNameMapType> jmap =
          dbrConfigJoinOn->getFieldNameMap();
      const auto joinIt = jmap->find(joinOnFieldUpperCase);

      if (it == map->end() or joinIt == jmap->end())
      {
        throw Fmi::Exception(BCP, "Operation processing failed!")
            .addDetail(fmt::format(
                "Joining database views '{}' and '{}' by using field name '{}' is not possible",
                m_dbrConfig.at(0)->getTableName(),
                dbrConfigJoinOn->getTableName(),
                joinOnField));
      }
    }

    m_dbrConfig.push_back(dbrConfigJoinOn);

    const auto typeOfJoinMapIt = typeOfJoinMap.find(typeOfJoin);

    if (typeOfJoinMapIt == typeOfJoinMap.end())
      throw Fmi::Exception(BCP, "Operation processing failed!")
          .addDetail(fmt::format("Type of join '{}' is not supported.", typeOfJoin));

    m_joinOnListTupleVector.emplace_back(JoinOnListTupleType(m_dbrConfig.at(0)->getTableName(),
                                                             dbrConfigJoinOn->getTableName(),
                                                             joinOnFields,
                                                             typeOfJoinMapIt->second));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void MastQueryParams::addField(const NameType& field, const NameType& alias)
{
  try
  {
    // Go through all the config files to find the field name.
    auto configIt = m_dbrConfig.cbegin();
    for (; configIt != m_dbrConfig.cend(); ++configIt)
    {
      const std::shared_ptr<DBRegistryConfig::FieldNameMapType> fieldNameMap =
          (*configIt)->getFieldNameMap();
      const std::string upperCase = Fmi::ascii_toupper_copy(field);
      const auto it = fieldNameMap->find(upperCase);

      if (it != fieldNameMap->end())
      {
        // Do not add duplicates.
        if (m_fields.find(upperCase) == m_fields.end())
        {
          m_fields.insert(
              std::pair<std::string, std::string>(it->first, (*configIt)->getTableName()));
          if (not alias.empty())
            m_fieldAliases.insert(std::pair<std::string, std::string>(it->first, alias));
        }
        return;
      }
    }

    throw Fmi::Exception(BCP, "Invalid parameter value!")
        .addDetail(fmt::format("Field name '{}' not found.", field));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void MastQueryParams::addOperation(const std::string& groupName,
                                   const NameType& field,
                                   const NameType& operationName,
                                   const boost::any& toWhat)
{
  try
  {
    // Is the conformance class creted.
    if (not m_conformanceClass)
      throw Fmi::Exception(BCP, "Operation processing failed!")
          .addDetail(
              fmt::format("MastQueryParams::addOperation operation '{}' not found", operationName));

    // is the operation
    std::shared_ptr<const PropertyIsBaseType> op =
        m_conformanceClass->getNewOperationInstance(field, operationName, toWhat);
    if (not op)
      throw Fmi::Exception(BCP, "Operation processing failed!")
          .addDetail(
              fmt::format("MastQueryParams::addOperation '{}' operation not found", operationName));

    // Find the table name where the field is defined.
    auto configIt = m_dbrConfig.cbegin();
    for (; configIt != m_dbrConfig.cend(); ++configIt)
    {
      const std::shared_ptr<DBRegistryConfig::FieldNameMapType> fieldNameMap =
          (*configIt)->getFieldNameMap();
      const std::string upperCase = Fmi::ascii_toupper_copy(field);
      const auto it = fieldNameMap->find(upperCase);

      // Add the operation if configuration is found.
      if (it != fieldNameMap->end())
      {
        // Matching input value type and the value type configured into the configuration of
        // database
        // view.
        // Only non empty configured value types are checked.
        const PropertyIsBaseType::NameType toWhatValueType = op->getValueTypeString();
        const DBRegistryConfig::NameType configuredValueType =
            (*configIt)->getFieldValueType(upperCase);
        if (not configuredValueType.empty() and toWhatValueType != configuredValueType)
        {
          std::cerr << "MastQueryParams::addOperation : value type '" << toWhatValueType << "'"
                    << " does not match with the configured value type '" << configuredValueType
                    << "'"
                    << " of field '" << upperCase << "' in " << (*configIt)->getTableName()
                    << "' view configuration while setting '" << operationName << "' operation.\n";
        }

        auto omIt = m_operationMap.find(groupName);
        if (omIt != m_operationMap.end())
        {
          omIt->second.insert(std::pair<std::shared_ptr<const PropertyIsBaseType>, NameType>(
              op, (*configIt)->getTableName()));
        }
        else
        {
          OperationMapGroupType omg;
          omg.insert(std::pair<std::shared_ptr<const PropertyIsBaseType>, NameType>(
              op, (*configIt)->getTableName()));
          m_operationMap.insert(std::pair<NameType, OperationMapGroupType>(groupName, omg));
        }
        return;
      }
    }

    throw Fmi::Exception(BCP, "Operation processing failed!")
        .addDetail(fmt::format(
            "MastQueryParams::addOperation the table not found that has '{}' field.", field));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void MastQueryParams::addOrderBy(const NameType& field, const NameType& ascDesc)
{
  try
  {
    bool validFieldName = false;
    const std::string fieldUpper = Fmi::ascii_toupper_copy(field);
    auto configIt = m_dbrConfig.cbegin();
    for (; configIt != m_dbrConfig.cend(); ++configIt)
    {
      const std::shared_ptr<DBRegistryConfig::FieldNameMapType> fieldNameMap =
          (*configIt)->getFieldNameMap();
      const auto it = fieldNameMap->find(fieldUpper);
      if (it != fieldNameMap->end())
      {
        validFieldName = true;
        break;
      }
    }

    if (not validFieldName)
      throw Fmi::Exception(BCP, "Operation processing failed!")
          .addDetail(fmt::format(
              "Trying to order SQL query result by using a field name '{}' that is not found from "
              "the configurations.",
              field));

    for (const auto& orderby : m_orderByVector)
    {
      if (orderby.first == fieldUpper)
        throw Fmi::Exception(BCP, "Operation processing failed!")
            .addDetail(fmt::format(
                "Trying to order SQL query result twice by using a field name '{}'.", field));
    }

    const std::string ascDescUpper = Fmi::ascii_toupper_copy(ascDesc);
    if (ascDescUpper != "ASC" and ascDescUpper != "DESC")
      throw Fmi::Exception(BCP, "Operation processing failed!")
          .addDetail(
              fmt::format("Invalid order '{}'. Only 'ASC' and 'DESC' are allowed.", ascDesc));

    m_orderByVector.emplace_back(std::pair<NameType, NameType>(fieldUpper, ascDescUpper));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

const std::shared_ptr<MastQueryParams::OperationMapType> MastQueryParams::getOperationMap() const
{
  try
  {
    return std::make_shared<MastQueryParams::OperationMapType>(m_operationMap);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

const std::shared_ptr<MastQueryParams::OrderByVectorType> MastQueryParams::getOrderByVector() const
{
  try
  {
    return std::make_shared<MastQueryParams::OrderByVectorType>(m_orderByVector);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

const std::shared_ptr<MastQueryParams::JoinOnListTupleVectorType>
MastQueryParams::getJoinOnListTupleVector() const
{
  try
  {
    return std::make_shared<MastQueryParams::JoinOnListTupleVectorType>(m_joinOnListTupleVector);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

MastQueryParams::NameType MastQueryParams::getTableName() const
{
  try
  {
    return m_dbrConfig.at(0)->getTableName();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::shared_ptr<MastQueryParams::FieldMapType> MastQueryParams::getFieldMap() const
{
  try
  {
    return std::make_shared<MastQueryParams::FieldMapType>(m_fields);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::shared_ptr<MastQueryParams::FieldAliasMapType> MastQueryParams::getFieldAliasMap() const
{
  try
  {
    return std::make_shared<MastQueryParams::FieldAliasMapType>(m_fieldAliases);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
