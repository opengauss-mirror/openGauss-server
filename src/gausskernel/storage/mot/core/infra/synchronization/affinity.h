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
 * affinity.h
 *    Utility class for managing thread affinity to cores by a configurable policy.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/synchronization/affinity.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef AFFINITY_H
#define AFFINITY_H

#include <stdint.h>
#include "type_formatter.h"

namespace MOT {
/** This class assumes that the mapping of processor ids and numa nodes is
 * organized as follows (where c is the number of cores per CPU and N the number
 * of NUMA nodes):
 * node 0: 0 1 2   ... (0*c+(c-1))    N*c    ... N*c+(c-1)
 * node 1: c (c+1) ... (1*c+(c-1))   (N+1)*c ... (N+1)*c+(c-1)
 * ...
 * node N: (N-1)*c ... (N-1)*c+(c-1) (2*N-1)*c ... 2*N*c-1
 * This matches current MOT server:
    node 0 cpus: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 192 193 194 195 196
 197 198 199 200 201 202 203 204 205 206 207 208 209 210 211 212 213 214 215
    node 1 cpus: 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 216 217 218
 219 220 221 222 223 224 225 226 227 228 229 230 231 232 233 234 235 236 237 238 239
    node 2 cpus: 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 240 241 242
 243 244 245 246 247 248 249 250 251 252 253 254 255 256 257 258 259 260 261 262 263
    node 3 cpus: 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 264 265 266
 267 268 269 270 271 272 273 274 275 276 277 278 279 280 281 282 283 284 285 286 287
    node 4 cpus: 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117
 118 119 288 289 290 291 292 293 294 295 296 297 298 299 300 301 302 303 304 305 306 307 308 309 310
 311
    node 5 cpus: 120 121 122 123 124 125 126 127 128 129 130 131 132 133 134 135 136 137 138 139 140
 141 142 143 312 313 314 315 316 317 318 319 320 321 322 323 324 325 326 327 328 329 330 331 332 333
 334 335
    node 6 cpus: 144 145 146 147 148 149 150 151 152 153 154 155 156 157 158 159 160 161 162 163 164
 165 166 167 336 337 338 339 340 341 342 343 344 345 346 347 348 349 350 351 352 353 354 355 356 357
 358 359
    node 7 cpus: 168 169 170 171 172 173 174 175 176 177 178 179 180 181 182 183 184 185 186 187 188
 189 190 191 360 361 362 363 364 365 366 367 368 369 370 371 372 373 374 375 376 377 378 379 380 381
 382 383
*/
/** @enum AffinityMode. Affinity mode that designates thread affinity policy among NUMA sockets. */
enum class AffinityMode {
    /** @var Fills the first socket, then the second, etc. */
    FILL_SOCKET_FIRST,

    /** @var Even number of threads for each socket (Hyper-threads are not used). */
    EQUAL_PER_SOCKET,

    /** @var Fills the first physical cores in first socket socket, then the second, etc. */
    FILL_PHYSICAL_FIRST,

    /** @var Constant value designating affinity settings are not in use (disabled). */
    AFFINITY_NONE,

    /** @var Constant value designating invalid affinity mode (indicates error in configuration loading). */
    AFFINITY_INVALID
};

/** @define Utility macro for checking whether affinity mode is usable. */
#define IS_AFFINITY_ACTIVE(affinity) \
    (((affinity) != MOT::AffinityMode::AFFINITY_NONE) && ((affinity) != MOT::AffinityMode::AFFINITY_INVALID))

/** @define Constant denoting an invalid CPU identifier. */
#define INVALID_CPU_ID ((uint32_t)-1)

/** @define Constant denoting an invalid NUMA node identifier. */
#define INVALID_NODE_ID ((uint32_t)-1)

/**
 * @class Affinity
 * @brief Utility class for managing thread affinity to cores by a configurable policy.
 */
class Affinity {
public:
    /**
     * Constructor.
     * @param numaNodes Number of NUMA nodes.
     * @param physicalCoresNuma Number of physical cores per NUMA node.
     * @param affinityMode The affinity mode to use.
     */
    Affinity(uint64_t numaNodes, uint64_t physicalCoresNuma, AffinityMode affinityMode);

    /**
     * @brief Configures the affinity parameters.
     * @param numaNodes Number of NUMA nodes.
     * @param physicalCoresNuma Number of physical cores per NUMA node.
     * @param affinityMode The affinity mode to use.
     */
    void Configure(uint64_t numaNodes, uint64_t physicalCoresNuma, AffinityMode affinityMode);

    /**
     * @brief Retrieves the identifier of the core affined to a given thread.
     * @param threadId The logical identifier of the thread.
     * @return The CPU identifier, or @ref INVALID_CPU_ID in case of failure.
     */
    uint32_t GetAffineProcessor(uint64_t threadId) const;

    /**
     * @brief Retrieves the identifier of the NUMA node affined to a given thread.
     * @param threadId The logical identifier of the thread.
     * @return The NUMA node identifier, or @ref INVALID_NODE_ID in case of failure.
     */
    uint32_t GetAffineNuma(uint64_t threadId) const;

    /**
     * @brief Sets the CPU affinity of a thread.
     * @param threadId The logical identifier of the thread.
     * @param[out,opt] threadCore The resulting core identifier.
     * @return True if operation succeeded, otherwise false.
     */
    bool SetAffinity(uint64_t threadId, uint32_t* threadCore = nullptr) const;

    /**
     * @brief Sets the CPU affinity of a thread to the specified NUMA node.
     * @param nodeId The NUMA node identifier.
     * @return True if operation succeeded, otherwise false.
     */
    bool SetNodeAffinity(int nodeId);

    /**
     * @brief Retrieves the configured affinity mode.
     * @return The affinity mode.
     */
    inline AffinityMode GetAffinityMode() const
    {
        return m_affinityMode;
    }

    /** @brief Sets the affinity mode. */
    inline void SetAffinityMode(AffinityMode affinityMode)
    {
        m_affinityMode = affinityMode;
    }

private:
    // class non-copy-able, non-assignable, non-movable
    /** @cond EXCLUDE_DOC */
    Affinity(const Affinity& orig) = delete;

    Affinity(const Affinity&& orig) = delete;

    Affinity& operator=(const Affinity& orig) = delete;

    Affinity& operator=(const Affinity&& orig) = delete;
    /** @endcond */

    /** @var Number of NUMA nodes in the computer. */
    uint64_t m_numaNodes;

    /** @var Number of physical cores per NUMA region. */
    uint64_t m_physicalCoresNuma;

    /** @var The affinity mode in use. */
    AffinityMode m_affinityMode;
};

/**
 * @brief Converts string value to affinity mode enumeration.
 * @param affinityModeStr The affinity string.
 * @return The affinity enumeration.
 */
extern AffinityMode AffinityModeFromString(const char* affinityModeStr);

/**
 * @brief Converts affinity mode enumeration into string form.
 * @param affinityMode The affinity mode.
 * @return The affinity mode string.
 */
extern const char* AffinityModeToString(AffinityMode affinityMode);

/**
 * @class TypeFormatter<AffinityMode>
 * @brief Specialization of TypeFormatter<T> with [ T = AffinityMode ].
 */
template <>
class TypeFormatter<AffinityMode> {
public:
    /**
     * @brief Converts a value to string.
     * @param value The value to convert.
     * @param[out] stringValue The resulting string.
     */
    static inline const char* ToString(const AffinityMode& value, mot_string& stringValue)
    {
        stringValue = AffinityModeToString(value);
        return stringValue.c_str();
    }

    /**
     * @brief Converts a string to a value.
     * @param The string to convert.
     * @param[out] The resulting value.
     * @return Boolean value denoting whether the conversion succeeded or not.
     */
    static inline bool FromString(const char* stringValue, AffinityMode& value)
    {
        value = AffinityModeFromString(stringValue);
        return value != AffinityMode::AFFINITY_INVALID;
    }
};
}  // namespace MOT
#endif /* AFFINITY_H */
