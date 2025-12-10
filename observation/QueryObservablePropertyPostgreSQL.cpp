#include "QueryObservablePropertyPostgreSQL.h"
#include "AsDouble.h"
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
QueryObservablePropertyPostgreSQL::~QueryObservablePropertyPostgreSQL() = default;

std::shared_ptr<std::vector<ObservableProperty> > QueryObservablePropertyPostgreSQL::executeQuery(
    PostgreSQLObsDB &db,
    const std::string &stationType,
    const std::vector<std::string> &parameters,
    const ParameterMapPtr &parameterMap,
    const std::string &language) const
{
  try
  {
    std::shared_ptr<std::vector<ObservableProperty> > observableProperties(
        new std::vector<ObservableProperty>);

    // Solving measurand id's for valid parameter aliases.
    ParameterIdMapType parameterIDs;
    solveMeasurandIds(parameters, parameterMap, stationType, parameterIDs);

    // Return empty list if some parameters are defined and any of those is
    // valid.
    if (parameterIDs.empty())
      return observableProperties;

    std::string sqlStmt =
        "SELECT meas.measurand_id, meas.measurand_code, TRIM( BOTH '-' FROM "
        "LOWER(meas.measurand_code || '-' || meas.standard_processing_duration || '-' || "
        "meas.standard_processing)) AS ObservableProperty_ID, coalesce(measL.measurand_name, "
        "meas.measurand_name) AS ObservableProperty_label, coalesce(bpL.PHENOMENON_NAME, "
        "bp.phenomenon_name) AS basePhenomenon, meas.measurand_unit AS uom, TRIM( BOTH '-' FROM "
        "LOWER(meas.standard_processing || '-' || meas.standard_processing_duration)) AS "
        "StatisticalMeasure_id, LOWER(meas.standard_processing) AS statisticalFunction, "
        "meas.standard_processing_duration as aggregationTimePeriod FROM measurand_v1 meas JOIN "
        "base_phenomenon_v1 bp  ON ( bp.base_phenomenon = meas.base_phenomenon ) LEFT OUTER JOIN "
        "measurand_v1l measL ON ( measL.measurand_id = meas.measurand_id AND  measL.language_code "
        "= 'fi' ) LEFT OUTER JOIN base_phenomenon_v1L bpL ON ( bpL.base_phenomenon = "
        "bp.base_phenomenon AND bpL.language_code = 'fi' ) ORDER BY 1;";

    boost::replace_all(sqlStmt, "fi", language);

    if (db.getDebug())
      std::cout << "PostgreSQL: " << sqlStmt << '\n';

    Fmi::Database::PostgreSQLConnection &connection = db.getConnection();

    pqxx::result result_set = connection.executeNonTransaction(sqlStmt);

    for (auto row : result_set)
    {
      int measurandId = -1;
      std::string measurandCode;
      std::string observablePropertyId;
      std::string observablePropertyLabel;
      std::string basePhenomenon;
      std::string uom;
      std::string statisticalMeasureId;
      std::string statisticalFunction;
      std::string aggregationTimePeriod;

      if (!row[0].is_null())
        measurandId = as_int(row[0]);
      if (!row[1].is_null())
        measurandCode = row[1].as<std::string>();
      if (!row[2].is_null())
        observablePropertyId = row[2].as<std::string>();
      if (!row[3].is_null())
        observablePropertyLabel = row[3].as<std::string>();
      if (!row[4].is_null())
        basePhenomenon = row[4].as<std::string>();
      if (!row[5].is_null())
        uom = row[5].as<std::string>();
      if (!row[6].is_null())
        statisticalMeasureId = row[6].as<std::string>();
      if (!row[7].is_null())
        statisticalFunction = row[7].as<std::string>();
      if (!row[8].is_null())
        aggregationTimePeriod = row[8].as<std::string>();

      // Multiple parameter name aliases may use a same measurand id (e.g. t2m
      // and temperature)
      auto r = parameterIDs.equal_range(measurandId);
      for (auto it = r.first; it != r.second; ++it)
      {
        ObservableProperty property;

        property.measurandId = Fmi::to_string(measurandId);
        property.measurandCode = measurandCode;
        property.observablePropertyId = observablePropertyId;
        property.observablePropertyLabel = observablePropertyLabel;
        property.basePhenomenon = basePhenomenon;
        property.uom = uom;
        property.statisticalMeasureId = statisticalMeasureId;
        property.statisticalFunction = statisticalFunction;
        property.aggregationTimePeriod = aggregationTimePeriod;
        property.gmlId = it->second;

        observableProperties->push_back(property);
      }
    }
    return observableProperties;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
