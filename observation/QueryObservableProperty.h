#pragma once

#include "ObservableProperty.h"
#include "ParameterMap.h"
#include "QueryBase.h"
#include <map>
#include <string>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class QueryObservableProperty : public QueryBase
{
 public:
  QueryObservableProperty() {}

  virtual ~QueryObservableProperty();

 protected:
  typedef std::multimap<int, std::string> ParameterIdMapType;
  typedef std::vector<std::string> ParameterVectorType;
  typedef std::string StationTypeType;

  void solveMeasurandIds(const ParameterVectorType &parameters,
                         const ParameterMapPtr &parameterMap,
                         const StationTypeType &stationType,
                         ParameterIdMapType &parameterIds) const;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
