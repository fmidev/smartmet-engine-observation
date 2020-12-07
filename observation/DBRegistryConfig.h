#pragma once

#include <boost/algorithm/string.hpp>
#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>
#include <macgyver/StringConversion.h>
#include <spine/ConfigBase.h>
#include <list>
#include <map>
#include <memory>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class DBRegistryConfig
{
 public:
  typedef std::string NameType;
  // 1. field name, 2. active (true / false)
  typedef std::map<NameType, bool> FieldNameMapType;
  // 1. fieldname
  // 2. method (e.g. the XMLType methods getClobVal(),
  //    getStringVal(), getNumberVal(), and getBlobVal(csid) to retrieve
  //    XML data as a CLOB, VARCHAR, NUMBER, and BLOB value, respectively.)
  typedef std::map<NameType, NameType> FieldMethodMapType;

  // 1. field name
  // 2. field value type (int, uint, float, double, string, ptime are allowed
  //                      and other strings are replaced with empty string)
  typedef std::map<NameType, NameType> FieldValueTypeMapType;

  DBRegistryConfig(std::shared_ptr<SmartMet::Spine::ConfigBase> config);

  /**
   * @brief Get the table (view) name.
   * @return Name of the table.
   */
  NameType getTableName() const { return m_name; }
  const std::shared_ptr<FieldNameMapType> getFieldNameMap() const
  {
    return std::make_shared<FieldNameMapType>(m_fieldNameMap);
  }

  /**
   * @brief Get data type configured for a field.
   * @param Field name as a string.
   * @return Data type configured for the field (or empty string in patological cases).
   */
  NameType getFieldValueType(const std::string& field) const
  {
    const std::string upperCase = Fmi::ascii_toupper_copy(field);
    FieldValueTypeMapType::const_iterator it = m_fieldValueTypeMap.find(upperCase);
    if (it != m_fieldValueTypeMap.end())
      return it->second;
    else
      return "";
  }

  /**
   * @brief Get method name of a field if defined.
   * @param fielName A field name which method to search.
   * @return Method name or empty return value.
   */
  NameType getMethod(const DBRegistryConfig::NameType& fielName) const;

 private:
  NameType m_name;
  FieldNameMapType m_fieldNameMap;
  FieldMethodMapType m_fieldMethodMap;
  FieldValueTypeMapType m_fieldValueTypeMap;
};

class NamesAllowed
{
 public:
  typedef DBRegistryConfig::FieldNameMapType NameMapType;
  typedef std::string NameType;
  typedef std::list<NameType> NameListType;

  /**
   *  @brief Contructor with input names and with optional case sensitivity selection.
   *  @param config Database table configuration.
   *  @param caseSensitiveNames Names are handled case sensitively (default) unless the param value
   * is false.
   */
  NamesAllowed(const std::shared_ptr<DBRegistryConfig> dbrConfig,
               const bool caseSensitiveNames = true);

  ~NamesAllowed();

  /**
   * @brief Add a name.
   * @param inName Name to add.
   * @retval true Input parameter name is added and is a valid name.
   * @retval false Input parameter name is not a valid name.
   */
  bool addName(const std::string& inName);

  const NameListType* getNameList() const { return &m_nameList; }

 private:
  NamesAllowed& operator=(const NamesAllowed& names);
  NamesAllowed(const NamesAllowed& names);

  NameMapType m_map;
  NameListType m_nameList;
  bool m_caseSensitiveNames;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

/**

@page OBSENGINE_DB_REGISTRY_CONFIG Database registry configuration

@section OBSENGINE_DB_REGISTRY_CONFIG_EXAMPLE_CONFIG_FILE Example configuration

- A name must begin with an alphabet character.
- A name is active unless it is deactivated.
- Allowed types are "int","uint","float","double","string" and "ptime" (optional).

@verbatim
name : "NETWORKS_V1";
fields :
(
{
        # Asemaverkon ID-numero.  number(4,0) not null
        name = "NETWORK_ID";
        type = "double"
},
{
        # Asemaverkon nimi. varchar2(128) not null
        name = "NETWORK_NAME";
        type = "string";
},
{
        # Milloin rivin tietoja on viimeksi muokattu. date not null
        name = "MODIFIED_LAST";
        active = false;
        type = "ptime";
}
);
@endverbatim

*/
