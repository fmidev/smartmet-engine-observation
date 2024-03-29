#include "Settings.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace
{
void print_vector(const std::vector<int>& data, const std::string& header, std::ostream& out)
{
  if (data.empty())
    return;

  out << header << std::endl;
  for (auto i : data)
    out << i << std::endl;
}
}  // anonymous namespace

std::ostream& operator<<(std::ostream& out, const Engine::Observation::Settings& settings)
{
  if (!settings.parameters.empty())
  {
    out << "parameters:" << std::endl;
    unsigned int i = 0;
    for (const auto& p : settings.parameters)
      out << "parameter #" << i++ << ": " << p.name() << std::endl;
  }
  else
    out << "parameters: none" << std::endl;

  if (!settings.taggedLocations.empty())
  {
    out << "taggedLocations:" << std::endl;
    unsigned int i = 0;
    for (const auto& l : settings.taggedLocations)
    {
      out << "taggedLocation #" << i++ << std::endl;
      out << "tag: " << l.tag << std::endl;
      out << Spine::formatLocation(*(l.loc));
    }
  }

  print_vector(settings.hours, "hours", out);
  print_vector(settings.weekdays, "weekdays", out);
  if (!settings.taggedFMISIDs.empty())
  {
    out << "fmisids" << std::endl;
    for (const auto& item : settings.taggedFMISIDs)
    {
      out << item.fmisid << std::endl;
    }
  }

  out << "locale: " << settings.locale.name() << std::endl;

  if (!settings.boundingBox.empty())
  {
    out << "boundingBox:" << std::endl;
    for (const auto& item : settings.boundingBox)
      out << item.first << " -> " << item.second << std::endl;
  }

  if (!settings.dataFilter.empty())
  {
    out << "dataFilter:" << std::endl;
    settings.dataFilter.print();
  }

  if (!settings.producer_ids.empty())
  {
    out << "producer_ids:" << std::endl;
    for (auto item : settings.producer_ids)
      out << item << std::endl;
  }
  else
    out << "producer_ids: none" << std::endl;

  out << "cacheKey: " << settings.cacheKey << std::endl;
  out << "format: " << settings.format << std::endl;
  out << "language: " << settings.language << std::endl;
  out << "localename: " << settings.localename << std::endl;
  out << "missingtext: " << settings.missingtext << std::endl;
  out << "stationtype: " << settings.stationtype << std::endl;
  out << "timeformat: " << settings.timeformat << std::endl;
  out << "timestring: " << settings.timestring << std::endl;
  out << "timezone: " << settings.timezone << std::endl;
  std::string wktString = settings.wktArea;
  if (wktString.size() > 50)
  {
    wktString.resize(50);
    wktString += " ... ";
  }
  std::cout << "wktArea: " << wktString << std::endl;
  out << "starttime: " << settings.starttime << std::endl;
  out << "endtime: " << settings.endtime << std::endl;
  if (settings.wantedtime)
    out << "wantedtime: " << *settings.wantedtime << std::endl;
  else
    out << "wantedtime: -" << std::endl;
  out << "maxdistance: " << settings.maxdistance << std::endl;
  out << "numberofstations: " << settings.numberofstations << std::endl;
  out << "timestep: " << settings.timestep << std::endl;
  out << "allplaces: " << settings.allplaces << std::endl;
  out << "starttimeGiven: " << settings.starttimeGiven << std::endl;
  out << "useCommonQueryMethod: " << settings.useCommonQueryMethod << std::endl;
  out << "useDataCache: " << settings.useDataCache << std::endl;
  out << "preventDatabaseQuery: " << settings.preventDatabaseQuery << std::endl;
  out << "requestLimits.maxlocations: " << settings.requestLimits.maxlocations << std::endl;
  out << "requestLimits.maxparameters: " << settings.requestLimits.maxparameters << std::endl;
  out << "requestLimits.maxtimes: " << settings.requestLimits.maxtimes << std::endl;
  out << "requestLimits.maxlevels: " << settings.requestLimits.maxlevels << std::endl;
  out << "requestLimits.maxelements: " << settings.requestLimits.maxelements << std::endl;
  out << "debug_options: " << settings.debug_options << std::endl;

  return out;
}
}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
