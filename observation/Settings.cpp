#include "Settings.h"

namespace SmartMet
{
void print_vector(const std::vector<int>& data, const std::string& header, std::ostream& out)
{
  if (data.size() == 0)
    return;

  out << header << std::endl;
  for (auto i : data)
    out << i << std::endl;
}

std::ostream& operator<<(std::ostream& out, const Engine::Observation::Settings& settings)
{
  if (settings.taggedLocations.size() > 0)
  {
    out << "taggedLocations:" << std::endl;
    unsigned int i = 0;
    for (auto l : settings.taggedLocations)
    {
      out << "taggedLocation #" << i++ << std::endl;
      out << "tag: " << l.tag << std::endl;
      out << SmartMet::Spine::formatLocation(*(l.loc));
    }
  }

  print_vector(settings.hours, "hours", out);
  print_vector(settings.weekdays, "weekdays", out);
  if (settings.taggedFMISIDs.size() > 0)
  {
    out << "fmisids" << std::endl;
    for (const auto& item : settings.taggedFMISIDs)
    {
      out << item.fmisid << std::endl;
    }
  }

  out << "locale: " << settings.locale.name() << std::endl;

  if (settings.boundingBox.size() > 0)
  {
    out << "boundingBox:" << std::endl;
    for (auto item : settings.boundingBox)
      out << item.first << " -> " << item.second << std::endl;
  }

  if (!settings.sqlDataFilter.empty())
  {
    out << "sqlDataFilter:" << std::endl;
    settings.sqlDataFilter.format(out);  // out << settings.sqlDataFilter;
  }

  if (settings.producer_ids.size() > 0)
  {
    out << "producer_ids:" << std::endl;
    for (auto item : settings.producer_ids)
      out << item << std::endl;
  }

  out << "cacheKey: " << settings.cacheKey << std::endl;
  out << "format: " << settings.format << std::endl;
  out << "language: " << settings.language << std::endl;
  out << "localename: " << settings.localename << std::endl;
  out << "missingtext: " << settings.missingtext << std::endl;
  out << "stationtype: " << settings.stationtype << std::endl;
  out << "timeformat: " << settings.timeformat << std::endl;
  out << "timestring: " << settings.timestring << std::endl;
  out << "timezone: " << settings.timezone << std::endl;
  out << "wktArea: " << settings.wktArea << std::endl;
  out << "starttime: " << settings.starttime << std::endl;
  out << "endtime: " << settings.endtime << std::endl;
  out << "maxdistance: " << settings.maxdistance << std::endl;
  out << "numberofstations: " << settings.numberofstations << std::endl;
  out << "timestep: " << settings.timestep << std::endl;
  out << "allplaces: " << settings.allplaces << std::endl;
  out << "latest: " << settings.latest << std::endl;
  out << "starttimeGiven: " << settings.starttimeGiven << std::endl;
  out << "useCommonQueryMethod: " << settings.useCommonQueryMethod << std::endl;
  out << "useDataCache: " << settings.useDataCache << std::endl;

  return out;
}
}  // namespace SmartMet