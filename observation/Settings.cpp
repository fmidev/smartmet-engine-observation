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

  out << header << '\n';
  for (auto i : data)
    out << i << '\n';
}
}  // anonymous namespace

std::ostream& operator<<(std::ostream& out, const Engine::Observation::Settings& settings)
{
  if (!settings.parameters.empty())
  {
    out << "parameters:\n";
    unsigned int i = 0;
    for (const auto& p : settings.parameters)
      out << "parameter #" << i++ << ": " << p.name() << '\n';
  }
  else
    out << "parameters: none\n";

  if (!settings.taggedLocations.empty())
  {
    out << "taggedLocations:\n";
    unsigned int i = 0;
    for (const auto& l : settings.taggedLocations)
    {
      out << "taggedLocation #" << i++ << '\n';
      out << "tag: " << l.tag << '\n';
      out << Spine::formatLocation(*(l.loc));
    }
  }

  print_vector(settings.hours, "hours", out);
  print_vector(settings.weekdays, "weekdays", out);
  if (!settings.taggedFMISIDs.empty())
  {
    out << "fmisids\n";
    for (const auto& item : settings.taggedFMISIDs)
    {
      out << item.fmisid << '\n';
    }
  }

  out << "locale: " << settings.locale.name() << '\n';

  if (!settings.boundingBox.empty())
  {
    out << "boundingBox:\n";
    for (const auto& item : settings.boundingBox)
      out << item.first << " -> " << item.second << '\n';
  }

  if (!settings.dataFilter.empty())
  {
    out << "dataFilter:\n";
    settings.dataFilter.print();
  }

  if (!settings.producer_ids.empty())
  {
    out << "producer_ids:\n";
    for (auto item : settings.producer_ids)
      out << item << '\n';
  }
  else
    out << "producer_ids: none\n";

  out << "cacheKey: " << settings.cacheKey << '\n';
  out << "format: " << settings.format << '\n';
  out << "language: " << settings.language << '\n';
  out << "localename: " << settings.localename << '\n';
  out << "missingtext: " << settings.missingtext << '\n';
  out << "stationtype: " << settings.stationtype << '\n';
  out << "timeformat: " << settings.timeformat << '\n';
  out << "timestring: " << settings.timestring << '\n';
  out << "timezone: " << settings.timezone << '\n';
  std::string wktString = settings.wktArea;
  if (wktString.size() > 50)
  {
    wktString.resize(50);
    wktString += " ... ";
  }
  std::cout << "wktArea: " << wktString << '\n';
  out << "starttime: " << settings.starttime << '\n';
  out << "endtime: " << settings.endtime << '\n';
  if (settings.wantedtime)
    out << "wantedtime: " << *settings.wantedtime << '\n';
  else
    out << "wantedtime: -\n";
  out << "maxdistance: " << settings.maxdistance << '\n';
  out << "numberofstations: " << settings.numberofstations << '\n';
  out << "timestep: " << settings.timestep << '\n';
  out << "allplaces: " << settings.allplaces << '\n';
  out << "starttimeGiven: " << settings.starttimeGiven << '\n';
  out << "useCommonQueryMethod: " << settings.useCommonQueryMethod << '\n';
  out << "useDataCache: " << settings.useDataCache << '\n';
  out << "preventDatabaseQuery: " << settings.preventDatabaseQuery << '\n';
  out << "requestLimits.maxlocations: " << settings.requestLimits.maxlocations << '\n';
  out << "requestLimits.maxparameters: " << settings.requestLimits.maxparameters << '\n';
  out << "requestLimits.maxtimes: " << settings.requestLimits.maxtimes << '\n';
  out << "requestLimits.maxlevels: " << settings.requestLimits.maxlevels << '\n';
  out << "requestLimits.maxelements: " << settings.requestLimits.maxelements << '\n';
  out << "debug_options: " << settings.debug_options << '\n';

  return out;
}
}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
