#include "MastQueryParams.h"
#include <macgyver/StringConversion.h>
#include <spine/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
MastQueryParams::MastQueryParams(const std::shared_ptr<DBRegistryConfig> dbrConfig)
    : m_distinct(false)
{
  try
  {
    if (!dbrConfig)
    {
      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail("Database registry configuration is not set.");
      throw exception;
    }

    m_dbrConfig.push_back(dbrConfig);
    m_conformanceClass = std::shared_ptr<ExtendedStandardFilter>(new ExtendedStandardFilter());
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

MastQueryParams::~MastQueryParams()
{
}

void MastQueryParams::addJoinOnConfig(const std::shared_ptr<DBRegistryConfig> dbrConfigJoinOn,
                                      const NameType& joinOnField,
                                      const int& typeOfJoin)
{
  try
  {
    std::list<NameType> joinOnFields;
    joinOnFields.push_back(joinOnField);
    addJoinOnConfig(dbrConfigJoinOn, joinOnFields, typeOfJoin);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void MastQueryParams::addJoinOnConfig(const std::shared_ptr<DBRegistryConfig> dbrConfigJoinOn,
                                      const std::list<NameType>& joinOnFields,
                                      const int& typeOfJoin)
{
  try
  {
    if (!dbrConfigJoinOn)
    {
      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail("Database registry configuration is not set.");
      throw exception;
    }
    if (joinOnFields.empty())
    {
      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(ObsEngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail("Join fields not set.");
      throw exception;
    }

    for (std::list<NameType>::const_iterator joinOnField = joinOnFields.begin();
         (joinOnField != joinOnFields.end());
         joinOnField++)
    {
      // Try to find the joinOnField from the first registry config. Fail if not found.
      const std::string joinOnFieldUpperCase = Fmi::ascii_toupper_copy(*joinOnField);
      const std::shared_ptr<DBRegistryConfig::FieldNameMapType> map =
          m_dbrConfig.at(0)->getFieldNameMap();
      DBRegistryConfig::FieldNameMapType::const_iterator it = map->find(joinOnFieldUpperCase);

      const std::shared_ptr<DBRegistryConfig::FieldNameMapType> jmap =
          dbrConfigJoinOn->getFieldNameMap();
      DBRegistryConfig::FieldNameMapType::const_iterator joinIt = jmap->find(joinOnFieldUpperCase);

      if (it == map->end() or joinIt == jmap->end())
      {
        std::ostringstream msg;
        msg << "Joining database views '" << m_dbrConfig.at(0)->getTableName() << "' and '"
            << dbrConfigJoinOn->getTableName() << "' by using field name '" << *joinOnField
            << "' is not possible";

        SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
        // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
        exception.addDetail(msg.str());
        throw exception;
      }
    }

    m_dbrConfig.push_back(dbrConfigJoinOn);

    TypeOfJoinMapType::const_iterator typeOfJoinMapIt = typeOfJoinMap.find(typeOfJoin);
    if (typeOfJoinMapIt == typeOfJoinMap.end())
    {
      std::ostringstream msg;
      msg << "Type of join '" << typeOfJoin << "' is not tupperted. ";

      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(ObsEngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg.str());
      throw exception;
    }

    m_joinOnListTupleVector.push_back(JoinOnListTupleType(m_dbrConfig.at(0)->getTableName(),
                                                          dbrConfigJoinOn->getTableName(),
                                                          joinOnFields,
                                                          typeOfJoinMapIt->second));
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void MastQueryParams::addField(const NameType& field, const NameType& alias)
{
  try
  {
    // Go through all the config files to find the field name.
    DBRegistryConfigVectorType::const_iterator configIt = m_dbrConfig.begin();
    for (; configIt != m_dbrConfig.end(); ++configIt)
    {
      const std::shared_ptr<DBRegistryConfig::FieldNameMapType> fieldNameMap =
          (*configIt)->getFieldNameMap();
      const std::string upperCase = Fmi::ascii_toupper_copy(field);
      DBRegistryConfig::FieldNameMapType::const_iterator it = fieldNameMap->find(upperCase);

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

    std::ostringstream msg;
    msg << "Field name '" << field << "' not found.";

    SmartMet::Spine::Exception exception(BCP, "Invalid parameter value!");
    // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
    exception.addDetail(msg.str());
    throw exception;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void MastQueryParams::addOperation(const std::string& groupName,
                                   const NameType& field,
                                   const NameType& operationName,
                                   const boost::any& toWhat)
{
  try
  {
    std::ostringstream msg;
    // Is the conformance class creted.
    if (not m_conformanceClass)
    {
      msg << "MastQueryParams::addOperation operation '" << operationName << "' not found\n";

      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg.str());
      throw exception;
    }

    // is the operation
    std::shared_ptr<const PropertyIsBaseType> op =
        m_conformanceClass->getNewOperationInstance(field, operationName, toWhat);
    if (not op)
    {
      msg << "MastQueryParams::addOperation '" << operationName << "' operation not found\n";

      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg.str());
      throw exception;
    }

    // Find the table name where the field is defined.
    DBRegistryConfigVectorType::const_iterator configIt = m_dbrConfig.begin();
    for (; configIt != m_dbrConfig.end(); ++configIt)
    {
      const std::shared_ptr<DBRegistryConfig::FieldNameMapType> fieldNameMap =
          (*configIt)->getFieldNameMap();
      const std::string upperCase = Fmi::ascii_toupper_copy(field);
      DBRegistryConfig::FieldNameMapType::const_iterator it = fieldNameMap->find(upperCase);

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

        OperationMapType::iterator omIt = m_operationMap.find(groupName);
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

    msg << "MastQueryParams::addOperation the table not found that has '" << field << "' field.\n";

    SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
    // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
    exception.addDetail(msg.str());
    throw exception;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void MastQueryParams::addOrderBy(const NameType& field, const NameType& ascDesc)
{
  try
  {
    bool validFieldName = false;
    const std::string fieldUpper = Fmi::ascii_toupper_copy(field);
    DBRegistryConfigVectorType::const_iterator configIt = m_dbrConfig.begin();
    for (; configIt != m_dbrConfig.end(); ++configIt)
    {
      const std::shared_ptr<DBRegistryConfig::FieldNameMapType> fieldNameMap =
          (*configIt)->getFieldNameMap();
      DBRegistryConfig::FieldNameMapType::const_iterator it = fieldNameMap->find(fieldUpper);
      if (it != fieldNameMap->end())
      {
        validFieldName = true;
        break;
      }
    }

    if (not validFieldName)
    {
      std::ostringstream msg;
      msg << "Trying to order SQL query result by using a field name '" << field
          << "' that is not found from the configurations.";

      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg.str());
      throw exception;
    }

    for (OrderByVectorType::iterator it = m_orderByVector.begin(); it != m_orderByVector.end();
         ++it)
    {
      if (it->first == fieldUpper)
      {
        std::ostringstream msg;
        msg << "Trying to order SQL query result twice by using a field name '" << field << "'.";

        SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
        // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
        exception.addDetail(msg.str());
        throw exception;
      }
    }

    const std::string ascDescUpper = Fmi::ascii_toupper_copy(ascDesc);
    if (ascDescUpper != "ASC" and ascDescUpper != "DESC")
    {
      std::ostringstream msg;
      msg << "Invalid order '" << ascDesc << "'. Only 'ASC' and 'DESC' are allowed.";

      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg.str());
      throw exception;
    }

    m_orderByVector.push_back(std::pair<NameType, NameType>(fieldUpper, ascDescUpper));
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

const std::shared_ptr<MastQueryParams::FieldMapType> MastQueryParams::getFieldMap() const
{
  try
  {
    return std::make_shared<MastQueryParams::FieldMapType>(m_fields);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

const std::shared_ptr<MastQueryParams::FieldAliasMapType> MastQueryParams::getFieldAliasMap() const
{
  try
  {
    return std::make_shared<MastQueryParams::FieldAliasMapType>(m_fieldAliases);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
