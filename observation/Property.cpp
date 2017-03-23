#include "Property.h"
#include <macgyver/StringConversion.h>
#include <spine/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace Property
{
Base::Base()
{
}

Base::~Base()
{
}

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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

Base::NameType Base::getExpression(const NameType& viewName) const
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
        .append(toWhatString(m_toWhat));

    return retVal;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

Base::NameType Base::toWhatString(const boost::any& value) const
{
  try
  {
    if ((value).type() == typeid(int32_t))
    {
      return Fmi::to_string(boost::any_cast<int32_t>(value));
    }
    else if ((value).type() == typeid(uint32_t))
    {
      return Fmi::to_string(boost::any_cast<uint32_t>(value));
    }
    else if ((value).type() == typeid(int64_t))
    {
      return Fmi::to_string(boost::any_cast<int64_t>(value));
    }
    else if ((value).type() == typeid(uint64_t))
    {
      return Fmi::to_string(boost::any_cast<uint64_t>(value));
    }
    else if ((value).type() == typeid(int16_t))
    {
      return Fmi::to_string(boost::any_cast<int16_t>(value));
    }
    else if ((value).type() == typeid(uint16_t))
    {
      return Fmi::to_string(static_cast<unsigned long>(boost::any_cast<uint16_t>(value)));
    }
    else if ((value).type() == typeid(float))
    {
      return Fmi::to_string(boost::any_cast<float>(value));
    }
    else if ((value).type() == typeid(double))
    {
      return Fmi::to_string(boost::any_cast<double>(value));
    }
    else if ((value).type() == typeid(std::string))
    {
      return "'" + boost::any_cast<std::string>(value) + "'";
    }
    else if ((value).type() == typeid(boost::posix_time::ptime))
    {
      return "TO_DATE('" +
             boost::posix_time::to_simple_string(boost::any_cast<boost::posix_time::ptime>(value)) +
             "','YYYY-MM-DD HH24:MI:SS')";
    }
    else
    {
      std::ostringstream msg;
      msg << "warning: SmartMet::Engine::Observation::Property::Base::toWhatString : Unsupported "
             "data "
             "type "
             "'"
          << (value).type().name() << "'.";

      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg.str());
      throw exception;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

Base::NameType MinuteValueModuloIsEqualToZero::getExpression(const Base::NameType& viewName) const
{
  try
  {
    Base::NameType retVal;
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

    return retVal;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

}  // namespace Property
}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
