#pragma once

#include "PostgreSQLObsDB.h"
#include "QueryObservableProperty.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class QueryObservablePropertyPostgreSQL : public QueryObservableProperty
{
 public:
  ~QueryObservablePropertyPostgreSQL() override;

  QueryObservablePropertyPostgreSQL() = default;
  QueryObservablePropertyPostgreSQL(const QueryObservablePropertyPostgreSQL& other) = default;
  QueryObservablePropertyPostgreSQL(QueryObservablePropertyPostgreSQL&& other) = default;
  QueryObservablePropertyPostgreSQL& operator=(const QueryObservablePropertyPostgreSQL& other) =
      default;
  QueryObservablePropertyPostgreSQL& operator=(QueryObservablePropertyPostgreSQL&& other) = default;

  std::shared_ptr<std::vector<ObservableProperty> > executeQuery(
      PostgreSQLObsDB& db,
      const std::string& stationType,
      const std::vector<std::string>& parameters,
      const ParameterMapPtr& parameterMap,
      const std::string& language) const;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
