/* -------------------------------------------------------------------------
 *
 * pgxc_node.h
 *	  definition of the system "PGXC node" relation (pgxc_node)
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/catalog/pgxc_node.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PGXC_NODE_H
#define PGXC_NODE_H

#include "catalog/genbki.h"

#define PgxcNodeRelationId  9015
#define PgxcNodeRelation_Rowtype_Id 11649

CATALOG(pgxc_node,9015) BKI_ROWTYPE_OID(11649) BKI_SHARED_RELATION BKI_SCHEMA_MACRO
{
	NameData	node_name;

	/*
	 * Possible node types are defined as follows
	 * Types are defined below PGXC_NODES_XXXX
	 */
	char		node_type;

	/*
	 * Port number of the node to connect to
	 */
	int4 		node_port;

	/*
	 * Host name of IP address of the node to connect to
	 */
	NameData	node_host;

	/*
	 * Port1 number of the node to connect to
	 */
	int4 		node_port1;

	/*
	 * Host1 name of IP address of the node to connect to
	 */
	NameData	node_host1;

	/*
	 * Is static config primary node primary
	 */
	bool		hostis_primary;

	/*
	 * Is this node primary
	 */
	bool		nodeis_primary;

	/*
	 * Is this node preferred
	 */
	bool		nodeis_preferred;

	/*
	 * Node identifier to be used at places where a fixed length node identification is required
	 */
	int4		node_id;

	/*
	 * Sctp port start number of the data node to connect to each ohter
	 */
	int4 		sctp_port;

	/*
	 * Stream control port start number of the data node to connect to each ohter
	 */
	int4 		control_port;

	/*
	 * Sctp port1 number of the data node to connect to each ohter
	 */
	int4 		sctp_port1;

	/*
	 * Stream control port start number of the data node to connect to each ohter
	 */
	int4 		control_port1;	

	bool        nodeis_central;

	bool		nodeis_active;
} FormData_pgxc_node;

typedef FormData_pgxc_node *Form_pgxc_node;

#define Natts_pgxc_node				16

#define Anum_pgxc_node_name			1
#define Anum_pgxc_node_type			2
#define Anum_pgxc_node_port			3
#define Anum_pgxc_node_host			4
#define Anum_pgxc_node_port1		5
#define Anum_pgxc_node_host1		6
#define Anum_pgxc_host_is_primary	7
#define Anum_pgxc_node_is_primary	8
#define Anum_pgxc_node_is_preferred	9
#define Anum_pgxc_node_id			10

#define Anum_pgxc_node_sctp_port	11
#define Anum_pgxc_node_strmctl_port	12
#define Anum_pgxc_node_sctp_port1	13
#define Anum_pgxc_node_strmctl_port1 14

#define Anum_pgxc_node_is_central 15
#define Anum_pgxc_node_is_active  16

/* Possible types of nodes */
#define PGXC_NODE_COORDINATOR		'C'
#define PGXC_NODE_DATANODE			'D'
#define PGXC_NODE_DATANODE_STANDBY  'S'
#define PGXC_NODE_NONE				'N'

#define IS_DATA_NODE(type) ((type) == PGXC_NODE_DATANODE || (type) == PGXC_NODE_DATANODE_STANDBY)
#define IS_PRIMARY_DATA_NODE(type) ((type) == PGXC_NODE_DATANODE)

#endif   /* PGXC_NODE_H */

