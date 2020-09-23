#include "QueryResult.h"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <macgyver/StringConversion.h>
#include <macgyver/Exception.h>
#include <limits>
#include <utility>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
QueryResult::QueryResult(const size_t& numberOfValueVectors)
    : m_numberOfValueVectors(numberOfValueVectors)
{
  try
  {
    m_valueContainer.resize(numberOfValueVectors);
    m_valueTypeOfVector.resize(numberOfValueVectors);
    m_valueVectorName.resize(numberOfValueVectors);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

QueryResult::QueryResult(const QueryResult& other)
    : QueryResultBase(0), m_numberOfValueVectors(other.m_numberOfValueVectors)
{
  try
  {
    std::copy(other.m_valueTypeOfVector.begin(),
              other.m_valueTypeOfVector.end(),
              std::back_inserter(m_valueTypeOfVector));
    std::copy(other.m_valueVectorName.begin(),
              other.m_valueVectorName.end(),
              std::back_inserter(m_valueVectorName));

    m_valueContainer.resize(other.m_valueContainer.size());
    for (size_t i = 0; i < m_valueContainer.size(); ++i)
    {
      std::copy(other.m_valueContainer.at(i).begin(),
                other.m_valueContainer.at(i).end(),
                std::back_inserter(m_valueContainer.at(i)));
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

QueryResult::~QueryResult() {}

QueryResult::ValueVectorType::const_iterator QueryResult::begin(const std::string& valueVectorName)
{
  try
  {
    size_t id = getValueVectorId(Fmi::ascii_toupper_copy(valueVectorName));
    return m_valueContainer.at(id).begin();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

QueryResult::ValueVectorType::const_iterator QueryResult::end(const std::string& valueVectorName)
{
  try
  {
    size_t id = getValueVectorId(Fmi::ascii_toupper_copy(valueVectorName));
    return m_valueContainer.at(id).end();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

size_t QueryResult::size(const std::string& valueVectorName)
{
  try
  {
    size_t id = getValueVectorId(Fmi::ascii_toupper_copy(valueVectorName));
    return m_valueContainer.at(id).end() - m_valueContainer.at(id).begin();
  }
  catch (...)
  {
    return 0;
  }
}

std::string QueryResult::toString(const ValueVectorType::const_iterator value,
                                  const uint32_t& precision)
{
  try
  {
    if (value->type() == typeid(int32_t))
    {
      return Fmi::to_string(boost::any_cast<int32_t>(*value));
    }
    else if (value->type() == typeid(uint32_t))
    {
      return Fmi::to_string(boost::any_cast<uint32_t>(*value));
    }
    else if (value->type() == typeid(int64_t))
    {
      return Fmi::to_string(boost::any_cast<int64_t>(*value));
    }
    else if (value->type() == typeid(uint64_t))
    {
      return Fmi::to_string(boost::any_cast<uint64_t>(*value));
    }
    else if (value->type() == typeid(int16_t))
    {
      return Fmi::to_string(boost::any_cast<int16_t>(*value));
    }
    else if (value->type() == typeid(uint16_t))
    {
      return Fmi::to_string(static_cast<unsigned long>(boost::any_cast<uint16_t>(*value)));
    }
    else if (value->type() == typeid(float))
    {
      return fmt::format("{:.{}f}", boost::any_cast<float>(*value), precision);
    }
    else if (value->type() == typeid(double))
    {
      return fmt::format("{:.{}f}", boost::any_cast<double>(*value), precision);
    }
    else if (value->type() == typeid(std::string))
    {
      return boost::any_cast<std::string>(*value);
    }
    else if (value->type() == typeid(boost::posix_time::ptime))
    {
      return Fmi::to_iso_extended_string(boost::any_cast<boost::posix_time::ptime>(*value)) + "Z";
    }
    else
    {
      throw Fmi::Exception(BCP, "Operation processing failed!")
          .addDetail(fmt::format("warning: QueryResult::toString : Unsupported data type '{}'",
                                 value->type().name()));
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::pair<double, double> QueryResult::minMax(const ValueVectorType::const_iterator beginIt,
                                              const ValueVectorType::const_iterator endIt)
{
  try
  {
    double min = std::numeric_limits<double>::max();
    double max = std::numeric_limits<double>::min();

    ValueVectorType::const_iterator it = beginIt;
    if (it == endIt)
      return std::make_pair(min, max);

    if ((*it).type() == typeid(float))
    {
      for (; it != endIt; ++it)
      {
        float val = boost::any_cast<float>(*it);
        if (val < min)
          min = static_cast<double>(val);
        if (val > max)
          max = static_cast<double>(val);
      }
    }
    else if ((*it).type() == typeid(double))
    {
      for (; it != endIt; ++it)
      {
        double val = boost::any_cast<double>(*it);
        if (val < min)
          min = val;
        if (val > max)
          max = val;
      }
    }

    return std::make_pair(min, max);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void QueryResult::getValueVectorData(const size_t& valueVectorId, ValueVectorType& outValueVector)
{
  try
  {
    if (m_numberOfValueVectors <= valueVectorId)
      throw Fmi::Exception(BCP, "Invalid parameter value!")
          .addDetail("QueryResult::set : value vector index is out of range.");

    outValueVector.resize(m_valueContainer.at(valueVectorId).size());

    // Take a copy.
    ValueVectorType::iterator first = m_valueContainer.at(valueVectorId).begin();
    ValueVectorType::iterator last = m_valueContainer.at(valueVectorId).end();
    ValueVectorType::iterator oFirst = outValueVector.begin();

    while (first != last)
    {
      *oFirst++ = *first++;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void QueryResult::getValueVectorData(const std::string& valueVectorName,
                                     ValueVectorType& outValueVector)
{
  try
  {
    size_t id = getValueVectorId(valueVectorName);
    getValueVectorData(id, outValueVector);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void QueryResult::getValueVectorData(const std::string& valueVectorName,
                                     std::vector<std::string>& outValueVector)
{
  try
  {
    size_t id = getValueVectorId(valueVectorName);
    getValueVectorData(id, outValueVector);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void QueryResult::getValueVectorData(const size_t& valueVectorId,
                                     std::vector<std::string>& outValueVector)
{
  try
  {
    if (m_numberOfValueVectors <= valueVectorId)
      throw Fmi::Exception(BCP, "Invalid parameter value!")
          .addDetail("QueryResult::set : value vector index is out of range.");

    outValueVector.resize(m_valueContainer.at(valueVectorId).size());

    // Take a copy.
    ValueVectorType::iterator first = m_valueContainer.at(valueVectorId).begin();
    ValueVectorType::iterator last = m_valueContainer.at(valueVectorId).end();
    std::vector<std::string>::iterator oFirst = outValueVector.begin();

    try
    {
      while (first != last)
      {
        if ((*first).type() == typeid(int32_t))
        {
          *oFirst++ = Fmi::to_string(boost::any_cast<int32_t>(*first++));
        }
        else if ((*first).type() == typeid(uint32_t))
        {
          *oFirst++ = Fmi::to_string(boost::any_cast<uint32_t>(*first++));
        }
        else if ((*first).type() == typeid(int64_t))
        {
          *oFirst++ = Fmi::to_string(boost::any_cast<int64_t>(*first++));
        }
        else if ((*first).type() == typeid(uint64_t))
        {
          *oFirst++ = Fmi::to_string(boost::any_cast<uint64_t>(*first++));
        }
        else if ((*first).type() == typeid(int16_t))
        {
          *oFirst++ = Fmi::to_string(boost::any_cast<int16_t>(*first++));
        }
        else if ((*first).type() == typeid(uint16_t))
        {
          *oFirst++ =
              Fmi::to_string(static_cast<unsigned long>(boost::any_cast<uint16_t>(*first++)));
        }
        else if ((*first).type() == typeid(float))
        {
          *oFirst++ = Fmi::to_string(boost::any_cast<float>(*first++));
        }
        else if ((*first).type() == typeid(double))
        {
          *oFirst++ = Fmi::to_string(boost::any_cast<double>(*first++));
        }
        else if ((*first).type() == typeid(std::string))
        {
          *oFirst++ = boost::any_cast<std::string>(*first++);
        }
        else if ((*first).type() == typeid(boost::posix_time::ptime))
        {
          *oFirst++ =
              Fmi::to_iso_extended_string(boost::any_cast<boost::posix_time::ptime>(*first++)) +
              "Z";
        }
        else if ((*first).empty())
        {
          *first++;
          *oFirst++ = "";
        }
        else
        {
          // Print only one warning
          if (first == m_valueContainer.at(valueVectorId).begin())
          {
            std::cerr
                << "warning: QueryResult::getValueVectorData : Unsupported data type '"
                << (*first).type().name()
                << "'. Conversion to string failed. Empty string is used insted of the data.\n";
          }
          *first++;
          *oFirst++ = "";
        }
      }
    }
    catch (const std::bad_cast& e)
    {
      throw Fmi::Exception(BCP, "Operation processing failed!")
          .addDetail(fmt::format("QueryResult::getValueVectorData : {}", e.what()));
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

size_t QueryResult::getValueVectorId(const std::string& valueVectorName)
{
  try
  {
    const std::string valueVectorNameUpper = Fmi::ascii_toupper_copy(valueVectorName);
    for (size_t id = 0; id < this->size(); ++id)
    {
      if (getValueVectorName(id) == valueVectorNameUpper)
        return id;
    }

    throw Fmi::Exception(BCP, "Invalid parameter value!")
        .addDetail(
            fmt::format("QueryResult::end : value vector name '{}' not found.", valueVectorName));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::string QueryResult::getValueVectorName(const size_t& valueVectorId)
{
  try
  {
    if (m_numberOfValueVectors <= valueVectorId)
      throw Fmi::Exception(BCP, "Invalid parameter value!")
          .addDetail("QueryResult::getValueVectorName : value vector index is out of range.");

    return m_valueVectorName.at(valueVectorId);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

bool QueryResult::set(const std::shared_ptr<QueryResultBase> input)
{
  try
  {
    if (not input)
      return false;
    try
    {
      // Must be a QueryResult object
      const std::shared_ptr<QueryResult> other = std::dynamic_pointer_cast<QueryResult>(input);
      if (not other)
      {
        std::cerr << "QueryResult::set : dynamic cast failed\n";
        return false;
      }

      // Same number of value vectors is required.
      if (m_numberOfValueVectors != other->m_numberOfValueVectors)
        return false;

      //
      // FIXME!! Value vector names (and types) must be equal.
      //

      // Overwrite the old value type data.
      m_valueTypeOfVector.clear();
      std::copy(other->m_valueTypeOfVector.begin(),
                other->m_valueTypeOfVector.end(),
                std::back_inserter(m_valueTypeOfVector));

      // Overwrite the old value vector names.
      m_valueVectorName.clear();
      std::copy(other->m_valueVectorName.begin(),
                other->m_valueVectorName.end(),
                std::back_inserter(m_valueVectorName));

      for (size_t i = 0; i < m_valueContainer.size(); ++i)
      {
        // Clear the old data.
        m_valueContainer.at(i).clear();

        // Insert the new data.
        std::copy(other->m_valueContainer.at(i).begin(),
                  other->m_valueContainer.at(i).end(),
                  std::back_inserter(m_valueContainer.at(i)));
      }
    }
    catch (const std::exception& e)
    {
      return false;
    }

    return true;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void QueryResult::set(const size_t& valueVectorId, const ValueType& value)
{
  try
  {
    if (m_numberOfValueVectors <= valueVectorId)
      throw Fmi::Exception(BCP, "Invalid parameter value!")
          .addDetail("QueryResult::set : value vector index is out of range.");

    // Store the first value and compare its type with the following ones.
    if (m_valueContainer[valueVectorId].size() == 0)
    {
      m_valueTypeOfVector[valueVectorId] = value;
    }
    else
    {
      if (m_valueTypeOfVector[valueVectorId].type() != value.type())
      {
        throw Fmi::Exception(BCP, "Invalid parameter value!")
            .addDetail(fmt::format("QueryResult::set : wrong data type '{}' with '{}'",
                                   value.type().name(),
                                   m_valueTypeOfVector[valueVectorId].type().name()));
      }
    }

    m_valueContainer[valueVectorId].push_back(value);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void QueryResult::setValueVectorName(const size_t& valueVectorId,
                                     const std::string& valueVectorName)
{
  try
  {
    if (m_numberOfValueVectors <= valueVectorId)
      throw Fmi::Exception(BCP, "Invalid parameter value!")
          .addDetail("QueryResult::setValueVectorName : value vector index is out of range.");

    m_valueVectorName.at(valueVectorId) = valueVectorName;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

size_t QueryResult::size() const
{
  return m_numberOfValueVectors;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
