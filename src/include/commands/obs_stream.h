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
 * ---------------------------------------------------------------------------------------
 * 
 * obs_stream.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/commands/obs_stream.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef OBS_STREAM_H
#define OBS_STREAM_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "lib/stringinfo.h"
#include "bulkload/utils.h"
#include "storage/parser.h"
#include "commands/copy.h"
#include "commands/gds_stream.h"
#include "storage/buf/buffile.h"

class OBSStream : public BulkLoadStream {
public:
    /* Constructor & De-constructor */
    OBSStream(FileFormat format, bool is_write);
    virtual ~OBSStream();

    /* Routines inherited from interface */
    void Initialize(const char* uri, CopyState cstate);

    void Close(); /* virtual function */

    int Read(); /* virtual function */

    int InternalRead(); /* virtual function */

    int ReadMessage(StringInfoData& dst); /* virtual function */

    int Write(void* src, Size len); /* virtual function */

    void Flush(); /* virtual function */

    void VerifyAddr();

    /* Getter & Setter */
    inline GDS::Parser* GetOBSParser() const
    {
        Assert(m_parser != NULL);
        return this->m_parser;
    };

    void set_parser_chunksize(uint32_t chunksize);
    void set_source_obs_copy_options(ObsCopyOptions* options);

    const ObsCopyOptions* get_obs_copy_options(void);
    void set_obs_copy_options(ObsCopyOptions* options);

    const char* StartNewSegment(void);

private:
    FileList m_filelist;

    /* Parser to handle OBS objects */
    GDS::Parser* m_parser;

    /* OBS object realted variables */
    string m_url;
    char* m_hostname;
    char* m_bucket;
    char* m_prefix;

    /* writer specific */
    bool m_write;
    BufFile* m_buffile;
    StringInfo m_cur_segment;
    size_t m_cur_segment_offset;
    int m_cur_segment_num;

    ObsCopyOptions* m_obs_options;
};

#endif
