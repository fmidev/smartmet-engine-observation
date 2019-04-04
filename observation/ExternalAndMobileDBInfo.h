#pragma once

#include <boost/date_time/posix_time/posix_time.hpp>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "ExternalAndMobileProducerConfig.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
enum class FieldType
{
  Double,
  String,
  Integer,
  DateTime,
  Unknown
};

struct FieldDescription
{
  FieldDescription(const std::string &fieldName, FieldType fieldType)
      : field_name(fieldName), field_type(fieldType)
  {
  }
  FieldDescription() : field_name(""), field_type(FieldType::Unknown) {}
  std::string field_name;
  FieldType field_type;
};

class ExternalAndMobileDBInfo
{
 public:
  ExternalAndMobileDBInfo(const ExternalAndMobileProducerMeasurand *producerMeasurand = nullptr);

  const std::vector<std::string> fieldNames() const;
  const std::map<std::string, unsigned int> &fieldIndexes() const;
  const FieldDescription &fieldDescription(const std::string &fieldName) const;
  std::set<std::string> keyFields() const;
  const std::map<std::string, unsigned int> &keyFieldIndexes() const;
  bool isKeyField(const std::string &fieldName) const;
  FieldType fieldType(const std::string &fieldName) const;
  std::string sqlSelect(const std::vector<int> &measurandIds,
                        const boost::posix_time::ptime &starttime,
                        const boost::posix_time::ptime &endtime,
                        const std::string &wktAreaFilter,
                        const std::map<std::string, std::vector<std::string>> &data_filter) const;
  std::string sqlSelectForCache(const std::string &producer,
                                const boost::posix_time::ptime &starttime) const;
  std::string sqlSelectFromCache(const std::vector<int> &measurandIds,
                                 const boost::posix_time::ptime &starttime,
                                 const boost::posix_time::ptime &endtime,
                                 const std::string &wktAreaFilter,
                                 const std::map<std::string, std::vector<std::string>> &data_filter,
                                 bool spatialite = false) const;

  static boost::posix_time::ptime epoch2ptime(double epoch);

 private:
  const ExternalAndMobileProducerMeasurand *itsProducerMeasurand{nullptr};
  std::vector<std::string> itsFieldNames;
  std::map<std::string, unsigned int> itsKeyFields;
  std::vector<FieldDescription> itsFieldDescription;
  std::map<std::string, unsigned int> itsFieldIndexes;
  FieldDescription itsEmptyField;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
