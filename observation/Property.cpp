#include "Property.h"
#include <tuple>
#include <boost/algorithm/string/join.hpp>
#include <fmt/format.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <macgyver/TypeMap.h>
#include <macgyver/TypeName.h>

namespace ba = boost::algorithm;

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace Property
{
Base::Base() = default;
Base::~Base() = default;
IsEqualTo::~IsEqualTo() = default;
IsLessThan::~IsLessThan() = default;
IsNotEqualTo::~IsNotEqualTo() = default;
IsLessThanOrEqualTo::~IsLessThanOrEqualTo() = default;
IsGreaterThan::~IsGreaterThan() = default;
IsGreaterThanOrEqualTo::~IsGreaterThanOrEqualTo() = default;
IsNull::~IsNull() = default;
IsNotNull::~IsNotNull() = default;
IsNil::~IsNil() = default;
IsLike::~IsLike() = default;
IsBetween::~IsBetween() = default;
MinuteValueModuloIsEqualToZero::~MinuteValueModuloIsEqualToZero() = default;

Base::Base(const Base& other)
{
  try
  {
    m_property = other.m_property;
    m_toWhat = other.m_toWhat;
    m_operator = other.m_operator;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

Base::NameType Base::getExpression(const NameType& viewName,
                                   const std::string& database /*= "oracle"*/) const
{
  try
  {
    std::string retVal;
    retVal.append(viewName)
        .append(".")
        .append(m_property)
        .append(" ")
        .append(m_operator)
        .append(" ")
        .append(toWhatString(m_toWhat, database));

    return retVal;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

namespace {
    typedef std::function<std::string(const boost::any&, const std::string)> value2str_t;

    template <typename ValueType>
    std::string value_vect2str(const boost::any& value, const std::string& database, value2str_t conv)
    {
        std::vector<std::string> parts;
        const std::vector<ValueType>& args = boost::any_cast<std::vector<ValueType> >(value);
        std::transform(
            args.begin(),
            args.end(),
            std::back_inserter(parts),
            [&database, &conv](const ValueType& x) { return conv(x, database); });
        return "(" + ba::join(parts, std::string(", ")) + ")";
    }

    template <typename ValueType>
    void add_type(Fmi::TypeMap<value2str_t>& type_map, value2str_t f)
    {
        type_map.add<ValueType>(f);
        type_map.add<std::vector<ValueType> >(
            [f](const boost::any& value, const std::string& database) -> std::string
            {
                return value_vect2str<ValueType>(value, database, f);
            });
    }

    template <typename ValueType>
    void add_type(Fmi::TypeMap<value2str_t>& type_map)
    {
        const auto f = [](const boost::any& value, const std::string&) -> std::string
                       {
                           return Fmi::to_string(boost::any_cast<ValueType>(value));
                       };
        type_map.add<ValueType>(f);
        type_map.add<std::vector<ValueType> >(
            [f](const boost::any& value, const std::string& database) -> std::string
            {
                return value_vect2str<ValueType>(value, database, f);
            });
    }

    template <typename ValueType>
    Fmi::TypeMap<value2str_t>& add_types(Fmi::TypeMap<value2str_t>& type_map)
    {
        add_type<ValueType>(type_map);
        return type_map;
    }

    template <typename ValueType, typename... RemainingTypes>
    typename std::enable_if_t<(sizeof...(RemainingTypes) > 0), Fmi::TypeMap<value2str_t>& >
    add_types(Fmi::TypeMap<value2str_t>& type_map)
    {
        add_type<ValueType>(type_map);
        if (sizeof...(RemainingTypes) > 0) {
            add_types<RemainingTypes...>(type_map);
        }
        return type_map;
    }

    Fmi::TypeMap<value2str_t> create_value_to_string_converter()
    {
        Fmi::TypeMap<value2str_t> conv;

        add_types<int16_t, uint16_t, int32_t, uint32_t, int64_t, uint64_t, float, double>(conv);

        add_type<boost::posix_time::ptime>(conv,
            [](const boost::any& value, const std::string& database) -> std::string
            {
                if (database == "oracle")
                    return "TO_DATE('" +
                        boost::posix_time::to_simple_string(
                            boost::any_cast<boost::posix_time::ptime>(value)) +
                        "','YYYY-MM-DD HH24:MI:SS')";
                else  // PostgreSQL
                    return boost::posix_time::to_simple_string(
                        boost::any_cast<boost::posix_time::ptime>(value));
            });
        return conv;
    };

}

Base::NameType Base::toWhatString(const boost::any& value,
                                  const std::string& database /*= "oracle"*/) const
{
  try {
     static auto converter_map = create_value_to_string_converter();

     value2str_t converter;

     try {
         converter = converter_map [value];
     } catch (...) {
         throw Fmi::Exception::Trace(BCP, "Warning: " + METHOD_NAME + ": Unsupported data type "
             + Fmi::demangle_cpp_type_name(value.type().name()));
     }

     return converter (value, database);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

Base::NameType Base::getValueTypeString() const
{
  try
  {
    if ((m_toWhat).type() == typeid(int32_t))
    {
      return "int";
    }
    else if ((m_toWhat).type() == typeid(uint32_t))
    {
      return "int";
    }
    else if ((m_toWhat).type() == typeid(int64_t))
    {
      return "int";
    }
    else if ((m_toWhat).type() == typeid(uint64_t))
    {
      return "int";
    }
    else if ((m_toWhat).type() == typeid(int16_t))
    {
      return "int";
    }
    else if ((m_toWhat).type() == typeid(uint16_t))
    {
      return "int";
    }
    else if ((m_toWhat).type() == typeid(float))
    {
      return "float";
    }
    else if ((m_toWhat).type() == typeid(double))
    {
      return "double";
    }
    else if ((m_toWhat).type() == typeid(std::string))
    {
      return "string";
    }
    else if ((m_toWhat).type() == typeid(boost::posix_time::ptime))
    {
      return "ptime";
    }
    else
    {
      return "unknown";
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

Base::NameType MinuteValueModuloIsEqualToZero::getExpression(
    const Base::NameType& viewName, const std::string& database /* = "oracle"*/) const
{
  try
  {
    Base::NameType retVal;
    if (database == "oracle")
      retVal.append("MOD(60*TO_CHAR(")
          .append(viewName)
          .append(".")
          .append(Base::getProperty())
          .append(",'HH24') + ")
          .append("TO_CHAR(")
          .append(viewName)
          .append(".")
          .append(Base::getProperty())
          .append(",'MI'), ")
          .append(toWhatString(Base::getToWhat()))
          .append(") ")
          .append(Base::getOperator())
          .append(" 0");
    else  // PostgreSQL
      retVal.append("MOD(60*EXTRACT(HOUR FROM ")
          .append(viewName)
          .append(".")
          .append(Base::getProperty())
          .append(") + ")
          .append(" EXTRACT(MINUTE FROM ")
          .append(viewName)
          .append(".")
          .append(Base::getProperty())
          .append("), ")
          .append(toWhatString(Base::getToWhat()))
          .append(") ")
          .append(Base::getOperator())
          .append(" 0");

    // (MOD(60*TO_CHAR(OBSERVATION_DATA_R1.DATA_TIME,'HH24') +
    // TO_CHAR(OBSERVATION_DATA_R1.DATA_TIME,'MI'), 10) = 0)

    return retVal;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

Base::NameType MinuteValueModuloIsEqualToZero::getValueTypeString() const
{
  try
  {
    if ((getToWhat()).type() == typeid(int16_t) or (getToWhat()).type() == typeid(int32_t) or
        (getToWhat()).type() == typeid(int64_t) or (getToWhat()).type() == typeid(uint16_t) or
        (getToWhat()).type() == typeid(uint32_t) or (getToWhat()).type() == typeid(uint64_t))
      return "ptime";
    else
      return "unknown";
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Property
}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
