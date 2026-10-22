# gms_i18n

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:25:18.396Z pushedAt=2026-09-12T05:53:54.083Z -->

## gms_i18n Overview

gms_i18n is a plugin based on openGauss that provides internationalization capabilities. The currently supported functions are: GMS_I18N.RAW_TO_CHAR and GMS_I18N.STRING_TO_RAW.

## gms_i18n Limitations

- Only the CREATE EXTENSION command is supported for loading the plugin.

## gms_i18n Installation

gms_i18n is included by default during openGauss packaging and compilation. After openGauss is installed, the extension can be loaded directly by executing create extension gms_i18n;.

## gms_i18n Usage

### Creating an Extension<a name="section21088306113"></a>

The gms_i18n extension can be created directly using the CREATE Extension command:

```
openGauss=# CREATE Extension gms_i18n;
```

### Using Extension<a name="section107391050141118"></a>

#### Function Declaration

- RAW_TO_CHAR(data IN RAW, src_charset IN VARCHAR2 DEFAULT NULL)
  Description: Converts RAW data from a valid character set into a VARCHAR string in the database character set.
  Parameter details: data: RAW type data; src_charset: source character set.
- GMS_I18N.STRING_TO_RAW(IN strdata varchar2, IN dst_chrset varchar2 DEFAULT NULL)
  Description: Converts a VARCHAR string into another valid character set and returns the result as raw data.
  Parameter details: strdata: the string to be converted; dst_chrset: destination character set.

#### Function Usage

raw_to_char Function

```sql
openGauss=# select gms_i18n.raw_to_char(hextoraw('616263646566C2AA'), 'utf8');
 raw_to_char 
-------------
 abcdefª
(1 row)

```

strin_to_raw Function

```sql
openGauss=# select gms_i18n.string_to_raw('abcdefª', 'utf8');
  string_to_raw   
------------------
 616263646566C2AA
(1 row)

```

### Dropping the Extension<a name="section1587441381220"></a>

The method for dropping the gms_i18n Extension in openGauss is as follows:

```
openGauss=# DROP Extension gms_i18n [CASCADE];
```

> [!NOTE] Note
>
> If the Extension is depended on by other objects, the CASCADE keyword must be added to drop all dependent objects.