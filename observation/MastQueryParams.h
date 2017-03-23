#ifndef MAST_QUERY_PARAMS_H
#define MAST_QUERY_PARAMS_H

#include <string>
#include <vector>
#include <tuple>
#include "QueryParamsBase.h"
#include "DBRegistryConfig.h"
#include "MinimumStandardFilter.h"
#include "StandardFilter.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
/**
 * @brief The class implements special parameter
 *        capabilities to Mast data.
 */
class MastQueryParams : public QueryParamsBase
{
 public:
  typedef DBRegistryConfig::NameType NameType;
  typedef std::map<NameType, NameType> FieldMapType;  //!< first: Field name, second: Table name
  typedef std::map<NameType, NameType> FieldAliasMapType;  //!< first: Field name, second: Field
  // alias name
  typedef FEConformanceClassBase::PropertyIsBaseType PropertyIsBaseType;

  typedef std::map<std::shared_ptr<const PropertyIsBaseType>, NameType> OperationMapGroupType;
  typedef std::map<NameType, OperationMapGroupType> OperationMapType;  //!< first: group name,
  //! second: (first: operation
  //! object, second: Table
  //! name)

  typedef std::vector<std::shared_ptr<DBRegistryConfig> > DBRegistryConfigVectorType;
  typedef std::tuple<NameType, NameType, NameType, NameType>
      JoinOnTupleType;  //!< 1: table name, 2: table name, 3: field name, 4: typeOfJoin
  typedef std::tuple<NameType, NameType, std::list<NameType>, NameType>
      JoinOnListTupleType;  //!< 1: table name, 2: table name, 3: field name list, 4: typeOfJoin
  typedef std::vector<JoinOnListTupleType> JoinOnListTupleVectorType;
  typedef std::vector<std::pair<NameType, NameType> > OrderByVectorType;

  typedef std::map<int, NameType> TypeOfJoinMapType;
  const TypeOfJoinMapType typeOfJoinMap = {{0, "INNER JOIN"},
                                           {1, "LEFT OUTER JOIN"},
                                           {2, "RIGHT OUTER JOIN"},
                                           {3, "CROSS JOIN"},
                                           {4, "NATURAL JOIN"}};

  /**
   * @brief Constructor with DB registy configuration.
   * @param dbrConfig A Pointer to the database table (or view) configuration.
   * @exception Obs_EngineException::OPERATION_PROCESSING_FAILED DB registry configuration is not
   * set.
   */
  explicit MastQueryParams(const std::shared_ptr<DBRegistryConfig> dbrConfig);

  ~MastQueryParams();

  /**
   * @bried Add a join on configuration.
   * @param dbrConfig A configuration to join on.
   * @param field A field name used on join (same name is required from the both configurations)
   * @param typeOfJoin A type of join operation (default is "INNER JOIN").
   * @exception Obs_EngineException::OPERATION_PROCESSING_FAILED If joining is not possible.
   */
  void addJoinOnConfig(const std::shared_ptr<DBRegistryConfig> dbrConfig,
                       const NameType& field,
                       const int& typeOfJoin = 0);
  void addJoinOnConfig(const std::shared_ptr<DBRegistryConfig> dbrConfig,
                       const std::list<NameType>& fields,
                       const int& typeOfJoin = 0);

  /**
   * @brief Add a field name to get data from a table defined in DBRegistryConfig.
   * Field name is a column name of a database table (or view) defined in DBRegistryConfig.
   * Field names allowed are the names in the DBRegistryConfig configurations added into
   * the class object in use. Optionally an alternate name can be used for the \a field
   * name by using \a alias parameter.
   * @param field A field name in the DBRegistryConfig configuration added into.
   * @param alias A user defined alias name for the \a field name (the value is ignored if an empty
   * string).
   * @exception Obs_EngineException::INVALID_PARAMETER_VALUE Field name is not found from the DB
   * registry configuration.
   */
  void addField(const NameType& field, const NameType& alias = "");

  /**
   * @brief Add an operation that is logically disconjuncted with the others with same field name.
   * @param groupName A user defined non empty string to group operations (E.g.
   * "OR_GROUP_station_id").
   *                  It is strongly recommend that a group name has the "OR_GROUP_" prefix.
   * @param field A field name of a DBRegistryConfig configuration in use.
   * @param operationName An operation name (eg. "PropertyIsEqualTo").
   * @param toWhat A value that is compared to the values behind selected \a field name.
   * @exception Obs_EngineException::OPERATION_PROCESSING_FAILED If operation addition fail.
   */
  void addOperation(const NameType& groupName,
                    const NameType& field,
                    const NameType& operationName,
                    const boost::any& toWhat);

  /**
   * @brief Set ascending or descending order for a field data.
   * Query Result will be ordered on the same order as orders added into.
   * @param field A field name in the DBRegistryConfig configuration added into.
   * @param ascOrDesc Ascending or descending order (allowed values are "ASC" and "DESC")
   */
  void addOrderBy(const NameType& field, const NameType& ascOrDesc);

  /**
   * @brief Remove duplicates.
   */
  void useDistinct() { m_distinct = true; }
  /**
   *
   */
  bool isDistinct() const { return m_distinct; }
  /**
   * @brief Access to the operation map of this object.
   * @return Reference to the object or empty shared_ptr.
   */
  const std::shared_ptr<OperationMapType> getOperationMap() const;

  /**
   * @brief Access to the order by map of this object.
   * @return Reference to the object or empty shared_ptr.
   */
  const std::shared_ptr<OrderByVectorType> getOrderByVector() const;

  /**
   * @brief Access to the join on tuple vector of this object.
   * @return Reference to the object or empty shared_ptr.
   */
  const std::shared_ptr<JoinOnListTupleVectorType> getJoinOnListTupleVector() const;

  /**
   * @brief Get the table name of primary DBRegistryConfig (constructor input config).
   * @return the name of table (or view).
   */
  NameType getTableName() const;

  /**
   * @brief Access to the field name map of this object.
   * @return Reference to the object or empty shared_ptr.
   */
  const std::shared_ptr<FieldMapType> getFieldMap() const;

  /**
   * @brief Access to the field name alias map of this object.
   * @return Reference to the object or empty shared_ptr.
   */
  const std::shared_ptr<FieldAliasMapType> getFieldAliasMap() const;

 private:
  MastQueryParams& operator=(const MastQueryParams& other);
  MastQueryParams(const MastQueryParams& other);

  DBRegistryConfigVectorType m_dbrConfig;
  FieldMapType m_fields;
  FieldAliasMapType m_fieldAliases;
  // Operations added into the class by using addOperation method.
  OperationMapType m_operationMap;
  std::shared_ptr<FEConformanceClassBase> m_conformanceClass;
  JoinOnListTupleVectorType m_joinOnListTupleVector;
  OrderByVectorType m_orderByVector;
  bool m_distinct;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

#endif  // ENVIRONMENTAL_MONITORING_FACILITY_QUERY_PARAMS_H
