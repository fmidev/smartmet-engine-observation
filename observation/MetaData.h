#pragma once

#include <macgyver/DateTime.h>
#include <spine/Value.h>
#include <map>
#include <set>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

typedef enum
{
  None,
  Pressure,
  Altitude
} ObsLevelType;

class ObservationLevel
{
 public:
  ObservationLevel() : levelType(ObsLevelType::None), levelValue(0.0) {};
  ObservationLevel(ObsLevelType type, double value) : levelType(type), levelValue(value) {};

  ObsLevelType getLevelType() const { return levelType; }
  double getLevelValue() const { return levelValue; }

  void setLevelType(ObsLevelType type) { levelType = type; }
  void setLevelValue(double value) { levelValue = value; }

 private:
  ObsLevelType levelType;
  double levelValue;
};

struct PeriodLevelMetaData
{
  PeriodLevelMetaData(const Fmi::TimePeriod &p) : period(p) {}
  PeriodLevelMetaData() : PeriodLevelMetaData(Fmi::TimePeriod(Fmi::DateTime(), Fmi::DateTime())) {}

  void update(const Fmi::DateTime &minTime,
              const Fmi::DateTime &maxTime,
              ObsLevelType levelType,
              double minLevel,
              double maxLevel)
  {
    if (period.is_null() || levels.empty())
      period = Fmi::TimePeriod(minTime, maxTime);
    else if (maxTime > period.end())
      period = Fmi::TimePeriod(period.begin(), maxTime);

    if (levels.empty())
    {
      levels.push_back(ObservationLevel(levelType, minLevel));
      levels.push_back(ObservationLevel(levelType, maxLevel));
    }
    else
    {
      if (minLevel < levels.front().getLevelValue())
        levels.front().setLevelValue(minLevel);
      if (maxLevel > levels.back().getLevelValue())
        levels.back().setLevelValue(maxLevel);
    }
  }

  Fmi::TimePeriod period;
  std::vector<ObservationLevel> levels;  // All levels or min/max range
};

typedef std::map<int, PeriodLevelMetaData> StationMetaData;

struct MetaData
{
  MetaData(Spine::BoundingBox b,
           const Fmi::TimePeriod &p,
           int step,
           ObsLevelType lt = ObsLevelType::None)
      : bbox(std::move(b)), period(p), timestep(step), levelType(lt), periodLevelMetaData(p)
  {
  }
  MetaData()
      : MetaData(Spine::BoundingBox(0.0, 0.0, 0.0, 0.0),
                 Fmi::TimePeriod(Fmi::DateTime(), Fmi::DateTime()),
                 1)
  {
  }
  MetaData(const MetaData &md) = default;
  MetaData &operator=(const MetaData &md) = default;
  MetaData(MetaData &&md) = default;
  MetaData &operator=(MetaData &&md) = default;

  void update(int stationId,
              const Fmi::DateTime &minTime,
              const Fmi::DateTime &maxTime,
              ObsLevelType levelType,
              double minLevel,
              double maxLevel)
  {
    periodLevelMetaData.update(minTime, maxTime, levelType, minLevel, maxLevel);

    StationMetaData::iterator smd = stationMetaData.find(stationId);
    if (smd == stationMetaData.end())
      smd = stationMetaData.insert(std::make_pair(stationId, PeriodLevelMetaData())).first;

    smd->second.update(minTime, maxTime, levelType, minLevel, maxLevel);

    hasLevelRange = true;
  }

  Spine::BoundingBox bbox;
  Fmi::TimePeriod period;
  const Fmi::TimePeriod &dbperiod() const { return periodLevelMetaData.period; }
  const Fmi::TimePeriod &dbperiod(int station_id) const
  {
    auto it = stationMetaData.find(station_id);
    return ((it != stationMetaData.end()) ? it->second.period : periodLevelMetaData.period);
  }
  bool fixedPeriodEndTime{false};
  int timestep = 1;  // timestep in minutes
  std::set<std::string> parameters;
  Fmi::DateTime latestDataUpdateTime;

  // Levels (soundings, mast data etc). levelType initially controls which level values are
  // loaded as level metadata. pressures (range) is default for soundings but could use
  // altitudes too
  //
  // Later when updating metadata, the level type is taken from the first (minumum) level value

  ObsLevelType levelType = ObsLevelType::None;
  const std::vector<ObservationLevel> &levels() const { return periodLevelMetaData.levels; }
  bool hasLevelRange = false;

  PeriodLevelMetaData periodLevelMetaData;
  StationMetaData stationMetaData;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
