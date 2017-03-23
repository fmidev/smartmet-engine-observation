#pragma once

#include <string>
#include <set>
#include <vector>
#include <map>
#include <memory>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class StationtypeConfig
{
 public:
  typedef std::string StationtypeType;
  typedef std::string GroupCodeType;
  typedef std::string DatabaseTableNameType;
  typedef uint ProducerIdType;
  typedef bool UseCommonQueryMethodType;
  typedef std::set<GroupCodeType> GroupCodeSetType;
  typedef std::vector<GroupCodeType> GroupCodeVectorType;
  typedef std::vector<ProducerIdType> ProducerIdVectorType;
  typedef std::set<ProducerIdType> ProducerIdSetType;
  typedef std::map<StationtypeType, GroupCodeSetType> STGroupCodeSetMapType;
  typedef std::map<StationtypeType, DatabaseTableNameType> STDatabaseTableNameMapType;
  typedef std::map<StationtypeType, UseCommonQueryMethodType> STUseCommonQueryMethodMapType;
  typedef std::map<StationtypeType, ProducerIdSetType> STProducerIdSetMapType;

  StationtypeConfig();
  ~StationtypeConfig();

  /**
   * @brief Add an stationtype configuration.
   * @param[in] stationtype Non zero length value.
   * @param[in] stationgroupVector At least one group code value required.
   * @exception std::runtime_error If an invalid input value is found.
   */
  void addStationtype(const StationtypeType& stationtype,
                      const GroupCodeVectorType& stationgroupVector);

  void setDatabaseTableName(const StationtypeType& stationtype,
                            const DatabaseTableNameType& databaseTableName);

  /**
   * @brief Use common query capability where only a database table name differs from each other.
   * @param[in] stationtype Stationtype keyword for the value.
   * @param[in] value Turn the use of common query method on or off.
   */
  void setUseCommonQueryMethod(const StationtypeType& stationtype,
                               const UseCommonQueryMethodType& value);

  /**
   * @brief Set producer ids for a stationtype.
   * @param[in] stationtype Stationtype keyword for the producerIdVector.
   * @param[in] producerIdVector Producer id list. An empty list will be iqnored.
   * @exception Obs_EngineException::INVALID_PARAMETER_VALUE If there is no a match with the
   * stationtype.
   */
  void setProducerIds(const StationtypeType& stationtype,
                      const ProducerIdVectorType& producerIdVector);

  /**
   * @brief Is the common query method enabled.
   * If a value is not set for a stationtype the return value is false.
   * @param[in] stationtype Stationtype keyword to search the state.
   * @retval true if common query method is enabled.
   * @retval true if common query method is not enabled.
   */
  UseCommonQueryMethodType getUseCommonQueryMethod(const StationtypeType& stationtype) const;

  /**
   * @brief Get database table name by using a stationtype.
   * @param[in] stationtype Stationtype keyword to search database table name.
   * @exception Obs_EngineException::INVALID_PARAMETER_VALUE If there is no a match with the
   * stationtype.
   * @return Reference to the database table nane.
   */
  std::shared_ptr<const DatabaseTableNameType> getDatabaseTableNameByStationtype(
      const StationtypeType& stationtype) const;

  /**
   * @brief Get a group code list by using a stationtype.
   * @param[in] stationtype Stationtype keyword to search group codes.
   * @exception Obs_EngineException::INVALID_PARAMETER_VALUE If there is no a match with the
   * stationtype.
   * @return Reference to the group code list match with the stationtype.
   */
  std::shared_ptr<const GroupCodeSetType> getGroupCodeSetByStationtype(
      const StationtypeType& stationtype) const;

  /**
   * @brief Get producer id set by using a stationtype.
   * @param[in] stationtype Stationtype keyword to search producer id set.
   * @exception Obs_EngineException::INVALID_PARAMETER_VALUE If there is no a match with the
   * stationtype.
   * @return Reference to the producer id set match with the stationtype.
   */
  std::shared_ptr<const ProducerIdSetType> getProducerIdSetByStationtype(
      const StationtypeType& stationtype) const;

  /**
   * @brief Get group code set map
   * @return Group code set map
   */
  const STGroupCodeSetMapType& getGroupCodeSetMap() const { return m_stationtypeMap; }

  /**
   * @brief Get database table name map
   * @return Database table name map
   */
  const STDatabaseTableNameMapType& getDatabaseTableNameMap() const { return m_stDatabaseTableNameMap; }

  /**
   * @brief Get common query method map
   * @return Common query method map
   */
  const STUseCommonQueryMethodMapType& getUseCommonQueryMethodMap() const { return m_stUseCommonQueryMethodMap; }

  /**
   * @brief Get producer id set map
   * @return Producer id set map
   */
  const STProducerIdSetMapType& getProducerIdSetMap() const { return m_stProducerIdSetMap; }

 private:
  StationtypeConfig& operator=(const StationtypeConfig& other);
  StationtypeConfig(const StationtypeConfig& other);

  STGroupCodeSetMapType m_stationtypeMap;
  STDatabaseTableNameMapType m_stDatabaseTableNameMap;
  STUseCommonQueryMethodMapType m_stUseCommonQueryMethodMap;
  STProducerIdSetMapType m_stProducerIdSetMap;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
