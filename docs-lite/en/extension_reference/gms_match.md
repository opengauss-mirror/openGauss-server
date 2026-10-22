# gms_match

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:25:38.064Z pushedAt=2026-09-12T06:14:26.706Z -->

## gms_match Overview

gms_match is an openGauss-based plugin used to compare the similarity between two strings. Currently supported interfaces include:

- gms_match.edit_distance
- gms_match.edit_distance_similarity

## gms_match Limitations

- The plugin can only be loaded using the create extension command.
- The gms_match plugin does not support set schema, meaning that executing `alter extension gms_match set schema new_name;` will result in an error.

## gms_match Installation

gms_match is included by default during the packaging and compilation of openGauss. After openGauss is installed, the extension can be loaded directly by executing `create extension gms_match;`.

## Using gms_match

### Creating an Extension<a name="section21088306113"></a>

The gms_match extension can be created directly using the create extension command:

```
create extension gms_match;
```

### Using Extension<a name="section107391050141118"></a>

#### gms_match.edit_distance

- gms_match.edit_distance(s1 in varchar2, s2 in varchar2) returns integer

  **Description**: This function returns the edit distance between two strings, i.e., the minimum number of changes required to transform s1 into s2, where a change refers to a single insertion, deletion, or substitution operation.

  **Parameter Description**:

  - `s1`: The first varchar2 input parameter, the source string.
  - `s2`: The first varchar2 input parameter, the target string

  **Return value**: integer data type, the edit distance between the two strings

  **Example**:

  ```
  select gms_match.edit_distance(NULL, 'ff');
  edit_distance 
  ---------------
              -1
  (1 row)

  select gms_match.edit_distance('', '');
  edit_distance 
  ---------------
              -1
  (1 row)

  select gms_match.edit_distance('', 'ab');
  edit_distance 
  ---------------
              -1
  (1 row)

  select gms_match.edit_distance('ab', 'ab');
  edit_distance 
  ---------------
              0
  (1 row)

  select gms_match.edit_distance('00', 'ff');
  edit_distance 
  ---------------
              2
  (1 row)

  select gms_match.edit_distance('ssttten', 'sitting');
  edit_distance 
  ---------------
              4
  (1 row)
  ```

#### gms_match.edit_distance_similarity

- gms_match.edit_distance_similarity(s1 in varchar2, s2 in varchar2) returns integer

  **Description**: This function returns the similarity between two strings, which is the normalized value of the edit distance (0 to 100). A larger value indicates higher similarity. The formula for the normalized value is: (1 - edit distance / max(length of the two parameters)) * 100.

  **Parameter Description**:

  - `s1`: The first varchar2 input parameter, the source string
  - `s2`: The first varchar2 input parameter, target string

  **Return value**: integer data type, the similarity between two strings

  **Example**:

  ```
  select gms_match.edit_distance_similarity(NULL, 'ff');
  edit_distance_similarity 
  --------------------------
                          0
  (1 row)

  select gms_match.edit_distance_similarity('', '');
  edit_distance_similarity 
  --------------------------
                        100
  (1 row)

  select gms_match.edit_distance_similarity('', 'ab');
  edit_distance_similarity 
  --------------------------
                          0
  (1 row)

  select gms_match.edit_distance_similarity('ab', 'ab');
  edit_distance_similarity 
  --------------------------
                        100
  (1 row)

  select gms_match.edit_distance_similarity('00', 'ff');
  edit_distance_similarity 
  --------------------------
                          0
  (1 row)

  select gms_match.edit_distance_similarity('ssttten', 'sitting');
  edit_distance_similarity 
  --------------------------
                        43
  (1 row)
  ```

### Deleting the Extension<a name="section1587441381220"></a>

The method for deleting the gms_match extension in openGauss is as follows:

```
drop extension gms_match [cascade];
```

>[!NOTE] Note
>
>If the extension is depended on by other objects, the cascade keyword must be added to delete all dependent objects.