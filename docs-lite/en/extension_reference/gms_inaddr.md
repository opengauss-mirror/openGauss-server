# gms_inaddr

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:25:32.701Z pushedAt=2026-09-24T10:53:40.095Z -->

## gms_inaddr Overview

gms_inaddr is an openGauss-based plugin that provides users with the capability to obtain host addresses or host names. The currently supported interfaces are: GMS_INADDR.GET_HOST_NAME and GMS_INADDR.GET_HOST_ADDRESS.

## gms_inaddr Limitations

- Only the CREATE EXTENSION command is supported for loading the plugin.
- The plugin can only be created in A-compatibility mode databases and supports full functionality only in this mode.

## gms_inaddr Installation

gms_inaddr is included by default during openGauss packaging and compilation. After installing openGauss, you can directly load the extension by using create extension gms_inaddr;.

## gms_inaddr Usage

### Creating an Extension<a name="section21088306113"></a>

To create the gms_inaddr extension, you can directly use the CREATE Extension command:

```
openGauss=# CREATE Extension gms_inaddr;
```

### Using Extension<a name="section107391050141118"></a>

#### Function Declaration

- GET_HOST_ADDRESS(text name default 'localhost')
  Description: This function obtains the corresponding host name based on the IP specified by the parameter.
  Parameter details: name: the host name for which the address needs to be obtained; status: if the retrieval is successful, this parameter returns the corresponding address; otherwise, an error is reported.
- GET_HOST_NAME(text ip default '127.0.0.1')
  Description: This function obtains the corresponding IP based on the host name specified by the parameter.
  Parameter details: ip: the address used to obtain the host name; if the retrieval is successful, the host name is returned; otherwise, an error is reported.

#### Using Functions

Test the get_host_name and get_host_addr functions

```
openGauss=# begin
openGauss$#   gms_output.enable;
openGauss$#   gms_output.put_line(gms_inaddr.get_host_address('localhost'));
openGauss$#   gms_output.put_line(gms_inaddr.get_host_name('127.0.0.1'));
openGauss$# end;
openGauss$# /
127.0.0.1
localhost
ANONYMOUS BLOCK EXECUTE
```

### Deleting an Extension<a name="section1587441381220"></a>

To delete the gms_inaddr extension in openGauss, use the following method:

```
openGauss=# DROP Extension gms_inaddr [CASCADE];
```

> [!NOTE] Note
>
> If the extension is depended on by other objects, you need to add the CASCADE keyword to delete all dependent objects.