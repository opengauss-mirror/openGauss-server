/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * ext_config_loader.h
 *    Configuration loader that loads from envelope.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/ext_config_loader.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef EXT_CONFIG_LOADER_H
#define EXT_CONFIG_LOADER_H

#include "config_loader.h"
#include "utilities.h"
#include "mot_error.h"
#include "typed_config_value.h"

namespace MOT {
/**
 * @class ExtConfigLoader
 * @brief Configuration loader that loads from envelope. This loader has the second highest
 * priority of all loaders.
 */
class ExtConfigLoader : public ConfigLoader {
public:
    /**
     * @brief Constructor.
     * @param name The name of the configuration loader.
     * tree or not.
     */
    explicit ExtConfigLoader(const char* name)
        : ConfigLoader(name, EXT_CONFIG_PRIORITY), m_extConfigTree(nullptr), m_hasChanged(false)
    {}

    /** @brief Destructor. */
    ~ExtConfigLoader() override
    {
        Cleanup();
    }

    /** @brief Notifies the loader that external configuration has changed.
     * @details The envelope should call this method whenever external configuration changes. In
     * return, the change flag in the loader is marked, so that next time the @ref ConfigMonitor
     * queries the loader for configuration changes, the answer will be positive (but only once). */
    inline void MarkChanged()
    {
        m_hasChanged = true;
    }

    /**
     * @brief Override virtual function in parent.
     * @return True if external envelope configuration changed (event trigger, not edge-trigger).
     */
    bool HasChanged() override
    {
        bool result = m_hasChanged;
        m_hasChanged = false;
        return result;
    }

    /**
     * @brief Loads external configuration.
     * @detail The envelope should override and implement this method to actually build the
     * configuration tree for this loader. If the purpose is to override configuration values loaded
     * by other loaders, then make sure to build a configuration tree with identical paths. After
     * making this call, make sure to call @ref markChanged().
     * @return True if succeeded, otherwise false.
     */
    bool LoadExtConfig();

protected:
    /**
     * @brief Implement configuration loading.
     * @return The resulting configuration tree or null pointer if failed.
     */
    ConfigTree* LoadConfig() override
    {
        ConfigTree* configTree = nullptr;
        if (LoadExtConfig()) {
            configTree = m_extConfigTree;
            m_extConfigTree = nullptr;  // must nullify, since during reload it is deleted in the layered config tree
        }
        return configTree;
    }

    /**
     * @brief Loads external configuration.
     * @detail The envelope should override and implement this method to actually build the
     * configuration tree for this loader. If the purpose is to override configuration values loaded
     * by other loaders, then make sure to build a configuration tree with identical paths.
     * @note While overriding this method, the envelope can call the utility helper functions devised
     * for this purpose as follows:
     *
     * <code>
     * ...
     * if (!AddExtStringConfigItem(...)) {
     *   return false;
     * }
     * if (!AddExtUInt64ConfigItem(...)) {
     *   return false;
     * }
     * ...
     * </code>
     *
     * @ref AddConfigItem().
     */
    virtual bool OnLoadExtConfig()
    {
        return true;
    }

    /**
     * @brief Helper function for adding a string configuration item.
     * @param path The full path from root.
     * @param key The key name.
     * @param value The value.
     * @return True if succeeded, otherwise false.
     */
    bool AddExtStringConfigItem(const char* path, const char* key, const char* value);

    /**
     * @brief Helper function for adding an unsigned 64 byte configuration item.
     * @param path The full path from root.
     * @param key The key name.
     * @param value The value.
     * @return True if succeeded, otherwise false.
     */
    bool AddExtUInt64ConfigItem(const char* path, const char* key, uint64_t value);

    /**
     * @brief Helper function for adding an unsigned 32 byte configuration item.
     * @param path The full path from root.
     * @param key The key name.
     * @param value The value.
     * @return True if succeeded, otherwise false.
     */
    bool AddExtUInt32ConfigItem(const char* path, const char* key, uint32_t value);

    /**
     * @brief Helper function for adding an unsigned 16 byte configuration item.
     * @param path The full path from root.
     * @param key The key name.
     * @param value The value.
     * @return True if succeeded, otherwise false.
     */
    bool AddExtUInt16ConfigItem(const char* path, const char* key, uint16_t value);

    /**
     * @brief Helper function for adding an unsigned 8 byte configuration item.
     * @param path The full path from root.
     * @param key The key name.
     * @param value The value.
     * @return True if succeeded, otherwise false.
     */
    bool AddExtUInt8ConfigItem(const char* path, const char* key, uint8_t value);

    /**
     * @brief Helper function for adding a signed 64 byte configuration item.
     * @param path The full path from root.
     * @param key The key name.
     * @param value The value.
     * @return True if succeeded, otherwise false.
     */
    bool AddExtInt64ConfigItem(const char* path, const char* key, int64_t value);

    /**
     * @brief Helper function for adding a signed 32 byte configuration item.
     * @param path The full path from root.
     * @param key The key name.
     * @param value The value.
     * @return True if succeeded, otherwise false.
     */
    bool AddExtInt32ConfigItem(const char* path, const char* key, int32_t value);

    /**
     * @brief Helper function for adding a signed 16 byte configuration item.
     * @param path The full path from root.
     * @param key The key name.
     * @param value The value.
     * @return True if succeeded, otherwise false.
     */
    bool AddExtInt16ConfigItem(const char* path, const char* key, int16_t value);

    /**
     * @brief Helper function for adding a signed 8 byte configuration item.
     * @param path The full path from root.
     * @param key The key name.
     * @param value The value.
     * @return True if succeeded, otherwise false.
     */
    bool AddExtInt8ConfigItem(const char* path, const char* key, int8_t value);

    /**
     * @brief Helper function for adding a double precision configuration item.
     * @param path The full path from root.
     * @param key The key name.
     * @param value The value.
     * @return True if succeeded, otherwise false.
     */
    bool AddExtDoubleConfigItem(const char* path, const char* key, double value);

    /**
     * @brief Helper function for adding a boolean configuration item.
     * @param path The full path from root.
     * @param key The key name.
     * @param value The value.
     * @return True if succeeded, otherwise false.
     */
    bool AddExtBoolConfigItem(const char* path, const char* key, bool value);

    /**
     * @brief Helper function for adding a typed configuration item.
     * @param path The full path from root.
     * @param key The key name.
     * @param value The value.
     * @return True if succeeded, otherwise false.
     * @note The type must have a TypeFormatter defined for it.
     */
    template <typename T>
    bool AddExtTypedConfigItem(const char* path, const char* key, const T& value)
    {
        mot_string str;
        return AddExtStringConfigItem(path, key, TypeFormatter<T>::ToString(value, str));
    }

private:
    /** @var The section map used to build the configuration tree. */
    ConfigSectionMap m_sectionMap;

    /** @var The list of all parsed section. */
    mot_list<ConfigSection*> m_parsedSections;

    /** @var Cached configuration. Loaded only once. Never modified. */
    ConfigTree* m_extConfigTree;

    /** @var Edge-triggered flag for notifying configuration changes. */
    bool m_hasChanged;

    /**
     * @brief Helper method for building external configuration tree. Call this method before making
     * any call to the other helper functions such as @ref AddExtStringConfigItem().
     */
    void BeginExtConfigLoad()
    {
        Cleanup();
    }

    /**
     * @brief Adds an external configuration item during configuration tree building.
     * @param configItem The configuration item.
     * @return True if succeeded, otherwise false.
     */
    bool AddExtConfigItem(ConfigItem* configItem);

    /**
     * @brief Helper method for building external configuration tree. Call this method after making
     * all calls to the other helper functions such as @ref AddExtStringConfigItem().
     * @return True if succeeded, otherwise false.
     */
    bool EndExtConfigLoad();

    /** Free all resources associated with the external loader. */
    void Cleanup();

    /**
     * @brief Helper function for adding a typed configuration item.
     * @param path The full path from root.
     * @param key The key name.
     * @param value The value.
     * @return True if succeeded, otherwise false.
     */
    template <typename T, typename V = TypedConfigValue<T>>
    inline bool AddTypedConfigItem(const char* path, const char* key, T value)
    {
        bool result = false;

        ConfigItem* configItem = CreateConfigValue<T, V>(path, key, value);
        if (configItem != nullptr) {
            result = AddExtConfigItem(configItem);
            if (!result) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Configuration",
                    "Failed to add external configuration item %s",
                    configItem->GetFullPathName());
                delete configItem;
            }
        } else {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate memory for configuration item");
        }

        return result;
    }

protected:
    DECLARE_CLASS_LOGGER()
};
}  // namespace MOT

#endif /* EXT_CONFIG_LOADER_H */
