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
 * xmltypes.h
 *
 * IDENTIFICATION
 *	  src\include\utils\xmltypes.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef XMLTYPES_H
#define XMLTYPES_H

typedef struct varlena xmltype;

typedef enum {
    XML_STANDALONE_YES,
    XML_STANDALONE_NO,
    XML_STANDALONE_NO_VALUE,
    XML_STANDALONE_OMITTED
}   XmlStandaloneType;

typedef enum {
    XMLBINARY_BASE64,
    XMLBINARY_HEX
}   XmlBinaryType;

typedef enum {
    PG_XML_STRICTNESS_LEGACY, /* ignore errors unless function result indicates error condition */
    PG_XML_STRICTNESS_WELLFORMED,       /* ignore non-parser messages */
    PG_XML_STRICTNESS_ALL       /* report all notices/warnings/errors */
} PgXmlStrictness;

/* struct PgXmlErrorContext is private to xml.c */
typedef struct PgXmlErrorContext PgXmlErrorContext;

#endif /* XMLTYPES_H */
