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
IsOneOf::~IsOneOf() = default;
IsNotOf::~IsNotOf() = default;
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

    struct TypeConv
    {
        value2str_t convert;
        std::string name;
        bool is_vector;
    };

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
    void add_type(Fmi::TypeMap<TypeConv>& type_map, value2str_t f, const std::string& type_name)
    {
        type_map.add<ValueType>({f, type_name, false});
        type_map.add<std::vector<ValueType> >(
            {
                [f](const boost::any& value, const std::string& database) -> std::string
                {
                    return value_vect2str<ValueType>(value, database, f);
                },
                type_name,
                true
            });
    }

    template <typename ValueType>
    void add_type(Fmi::TypeMap<TypeConv>& type_map, const std::string& type_name)
    {
        const auto f = [](const boost::any& value, const std::string&) -> std::string
                       {
                           return Fmi::to_string(boost::any_cast<ValueType>(value));
                       };
        type_map.add<ValueType>({f, type_name, false});
        type_map.add<std::vector<ValueType> >(
            {
                [f](const boost::any& value, const std::string& database) -> std::string
                {
                    return value_vect2str<ValueType>(value, database, f);
                },
                type_name,
                true
            });
    }

    template <typename ValueType>
    Fmi::TypeMap<TypeConv>& add_types(Fmi::TypeMap<TypeConv>& type_map, const std::string& type_name)
    {
        add_type<ValueType>(type_map, type_name);
        return type_map;
    }

    template <typename ValueType, typename... RemainingTypes>
    typename std::enable_if<(sizeof...(RemainingTypes) > 0), Fmi::TypeMap<TypeConv>& >::type
    add_types(Fmi::TypeMap<TypeConv>& type_map, const std::string& type_name)
    {
        add_type<ValueType>(type_map, type_name);
        if (sizeof...(RemainingTypes) > 0) {
            add_types<RemainingTypes...>(type_map, type_name);
        }
        return type_map;
    }

    Fmi::TypeMap<TypeConv> create_value_to_string_converter()
    {
        Fmi::TypeMap<TypeConv> conv;

        add_types<int16_t, uint16_t, int32_t, uint32_t, int64_t, uint64_t>(conv, "int");

        add_types<float>(conv, "float");

        add_types<double>(conv, "double");

        add_type<std::string>(conv,
            [](const boost::any& value, const std::string&) -> std::string
            {
                return "'" + boost::any_cast<std::string>(value) + "'";
            },
            "string");

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
            },
            "ptime");
        return conv;
    };

    Fmi::TypeMap<TypeConv>& get_value_to_string_converter_map()
    {
        static auto converter_map = create_value_to_string_converter();
        return converter_map;
    }

}

Base::NameType Base::toWhatString(const boost::any& value,
                                  const std::string& database /*= "oracle"*/) const
{
  try
  {
    static auto converter_map = get_value_to_string_converter_map();

    value2str_t converter;

    try
    {
       const auto& conv_info = converter_map [value];
       converter = conv_info.convert;
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
  static auto converter_map = get_value_to_string_converter_map();

  try
  {
    return converter_map [m_toWhat] . name;
  }
  catch (...)
  {
    return "unknown";
  }
}

void Base::set(const NameType& property, const boost::any& toWhat, const NameType& op)
{
  // Verify that provided value (toWhat) is compatible with operation
  static auto converter_map = get_value_to_string_converter_map();
  const auto& conv_info = converter_map [toWhat];
  if (conv_info.is_vector ^ has_vector_argument()) {
    throw Fmi::Exception(BCP,
        std::string("Argument type conflict - required ")
        + (has_vector_argument() ? "vector" : "scalar")
        + ", got " + (conv_info.is_vector ? "vector" : "scalar"));
  }

  m_property = property;
  m_toWhat = toWhat;
  m_operator = op;
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
