#ifndef VERIFIABLE_MESSAGE_QUERY_PARAMS_H
#define VERIFIABLE_MESSAGE_QUERY_PARAMS_H

#include <string>
#include <vector>
#include <list>
#include <unordered_map>
#include <map>
#include <boost/assign.hpp>
#include "QueryParamsBase.h"
#include "DBRegistryConfig.h"

namespace SmartMet {
namespace Engine {
namespace Observation {
/**
 * @brief The class implements special parameter
 *        capabilities to fetch IWXXM data.
 *
 * The class is designed to relay parameters and
 * some additional guidance to VerifiableMessageQuery
 * class where those are used in SQL statement contruction.
 */
class VerifiableMessageQueryParams : public QueryParamsBase {
public:
  /**
   * @brief The enum defines keywords that restricts
   *        (or widens) the way of use of parameters.
   */
  enum Restriction : int {
    RETURN_ONLY_LATEST
  };

  typedef DBRegistryConfig::NameType NameType;
  typedef NameType StationIdType;
  typedef std::vector<StationIdType> StationIdVectorType;
  typedef DBRegistryConfig::NameType SelectNameType;
  typedef std::list<SelectNameType> SelectNameListType;
  typedef DBRegistryConfig::NameType TableNameType;

  explicit VerifiableMessageQueryParams(
      const std::shared_ptr<DBRegistryConfig> dbrConfig);

  ~VerifiableMessageQueryParams();

  /**
   * @brief Add name used in SQL select statement.
   * @param selectName Name of the column in database.
   * @retval true Input parameter name is added and is a valid name.
   * @retval false Input parameter name is not a valid name.
   */
  bool addSelectName(const std::string &selectName);

  /**
   * @brief Add a station identifier (e.g. EFHK)
   */
  void addStationId(const std::string &stationId);

  /**
   * @brief Get the list reference for select names added into.
   * @return Reference to the select name list or an empty list.
   */
  const SelectNameListType *getSelectNameList() const;

  /**
   * @brief Get the reference for the station identifier vector.
   * @return Reference to the station identifier vector.
   */
  StationIdVectorType *getStationIdVector() const;

  /**
   * @brief Get a table name.
   * @return The table name of the object or empty string if the name is
   * missing.
   */
  TableNameType getTableName() const;

  /**
   * @brief Get method name to retrieve data as an other type.
   * E.g. XML data as a CLOB value.
   * @param Select name to search the method.
   * @return Name of a method or empty value.
   */
  NameType getSelectNameMethod(const NameType &name) const;

  /**
   * @brief Test if a restriction is set on.
   * @retval true Restriction is set on.
   * @retval false Restriction is off.
   */
  bool isRestriction(int id) const;

  /**
	 * @brief Set the restriction on.
	 */ // FIXME!! replace the method with setRestriction(int id)
  void setReturnOnlyLatest();

private:
  VerifiableMessageQueryParams &
  operator=(const VerifiableMessageQueryParams &other);
  VerifiableMessageQueryParams(const VerifiableMessageQueryParams &other);

  std::shared_ptr<DBRegistryConfig> m_dbrConfig;

  StationIdVectorType *m_stationIdVector;
  NamesAllowed *m_namesAllowed;

  std::unordered_map<int, bool> m_restrictionMap;
};

} // namespace Observation
} // namespace Engine
} // namespace SmartMet

#endif // VERIFIABLE_MESSAGE_QUERY_PARAMS_H
