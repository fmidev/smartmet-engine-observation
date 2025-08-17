#pragma once

#include <macgyver/DateTime.h>
#include <macgyver/StringConversion.h>
#include <optional>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

// Self assigned numbers for the producers
const int ForeignProducer = 1001;
const int RoadProducer = 1002;

class DataItem
{
 public:
  // If you add new data members don't forget to change hash_value()
  Fmi::DateTime data_time;
  Fmi::DateTime modified_last;
  std::optional<double> data_value;
  int fmisid = 0;
  int sensor_no = 1; // default sensor_no at FMI
  int measurand_id = 0;
  int producer_id = 0;
  int measurand_no = 1;
  int data_quality = 0;
  int data_source = -1;  // -1 indicates NULL value

  std::size_t hash_value() const;
  std::string get_value() const;
  std::string get_data_source() const;

  bool operator==(const DataItem& other) const;  // fixed metadata comparison only
  bool operator<(const DataItem& other) const;   // also compares modified_last
};

using DataItems = std::vector<DataItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

std::ostream& operator<<(std::ostream& out, const SmartMet::Engine::Observation::DataItem& item);
