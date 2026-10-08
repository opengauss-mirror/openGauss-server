# shark

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:38:16.645Z pushedAt=2026-09-29T02:34:56.705Z -->

## shark Overview

openGauss provides the shark extension (version: shark-1.0.0), an extension of the openGauss D-compatible database (dbcompatibility='D'), which aims to be compatible with D database syntax. The shark extension inherits the original SQL syntax of the kernel. [shark Syntax](shark_keywords.md) describes the syntax that is added to or modified from the kernel syntax. Syntax that is the same as the kernel syntax is not described.

## shark Restrictions

- The shark extension can be created only in a D-compatible database.
- By default, the shark extension cannot be deleted. However, if the parameter `support_extended_features` is enabled and no dependencies exist, deletion of the extension is allowed.
- If the shark extension has been created, add shark to the GUC parameter `shared_preload_libraries` before restarting or upgrading the database. Otherwise, you cannot connect to the D-compatible database or the upgrade fails.
- For all new/modified syntax in shark, the help information cannot be viewed on the gsql client using ```\h```, and syntax auto-completion is not supported on the gsql client.
- Currently, shark can be used only in databases created using the UTF8 or SQL_ASCII character set.

## Installing shark

The shark extension is compiled together with the kernel. You need to manually create the extension. The steps are as follows:

### Compile and Install

1. [Compile and install openGauss](https://gitcode.com/opengauss/openGauss-server#%E7%BC%96%E8%AF%91).

2. Create the D-compatible database and create the extension.

```
openGauss=# create database db_name dbcompatibility 'D';
CREATE DATABASE

openGauss=# \c db_name

db_name=# create extension shark ;
CREATE EXTENSION
```
