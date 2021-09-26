/*
 * psql - the openGauss interactive terminal
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 * src/bin/psql/variables.c
 */
#include "common.h"
#include "postgres_fe.h"
#include "variables.h"


/*
 * Check whether a variable's name is allowed.
 *
 * We allow any non-ASCII character, as well as ASCII letters, digits, and
 * underscore.	Keep this in sync with the definition of variable_char in
 * psqlscan.l.
 */
static bool valid_variable_name(const char* name)
{
    const unsigned char* ptr = (const unsigned char*)name;

    /* Mustn't be zero-length */
    if (*ptr == '\0') {
        return false;
    }
    while (*ptr) {
        if (IS_HIGHBIT_SET(*ptr) || strchr("ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                           "abcdefghijklmnopqrstuvwxyz"
                                           "_0123456789",
                                        *ptr) != NULL)
            ptr++;
        else
            return false;
    }

    return true;
}

/*
 * A "variable space" is represented by an otherwise-unused struct _variable
 * that serves as list header.
 */
VariableSpace CreateVariableSpace(void)
{
    struct _variable* ptr = NULL;

    ptr = (struct _variable*)pg_malloc(sizeof(*ptr));
    ptr->name = NULL;
    ptr->value = NULL;
    ptr->assign_hook = NULL;
    ptr->next = NULL;

    return ptr;
}

const char* GetVariable(VariableSpace space, const char* name)
{
    struct _variable* current = NULL;

    if (space == NULL)
        return NULL;

    for (current = space->next; current != NULL; current = current->next) {
        if (strcmp(current->name, name) == 0) {
            /* this is correct answer when value is NULL, too */
            return current->value;
        }
    }

    return NULL;
}

/*
 * Try to interpret value as boolean value.  Valid values are: true,
 * false, yes, no, on, off, 1, 0; as well as unique prefixes thereof.
 */
bool ParseVariableBool(const char* value)
{
    size_t len;

    if (value == NULL)
        return false; /* not set -> assume "off" */

    len = strlen(value);
    if (pg_strncasecmp(value, "true", len) == 0)
        return true;
    else if (pg_strncasecmp(value, "false", len) == 0)
        return false;
    else if (pg_strncasecmp(value, "yes", len) == 0)
        return true;
    else if (pg_strncasecmp(value, "no", len) == 0)
        return false;
    /* 'o' is not unique enough */
    else if (pg_strncasecmp(value, "on", (len > 2 ? len : 2)) == 0)
        return true;
    else if (pg_strncasecmp(value, "off", (len > 2 ? len : 2)) == 0)
        return false;
    else if (pg_strcasecmp(value, "1") == 0)
        return true;
    else if (pg_strcasecmp(value, "0") == 0)
        return false;
    else {
        /* NULL is treated as false, so a non-matching value is 'true' */
        psql_error("unrecognized Boolean value; assuming \"on\"\n");
        return true;
    }
    /* suppress compiler warning */
    return true;
}

/*
 * Read numeric variable, or defaultval if it is not set, or faultval if its
 * value is not a valid numeric string.  If allowtrail is false, this will
 * include the case where there are trailing characters after the number.
 */
int ParseVariableNum(const char* val, int defaultval, int faultval, bool allowtrail)
{
    int result;

    if (val == NULL) {
        result = defaultval;
    } else if (!val[0]) {
        result = faultval;
    }
    else {
        char* end = NULL;

        result = (int)strtol(val, &end, 0);
        if (!allowtrail && *end) {
            result = faultval;
        }
    }

    return result;
}

int GetVariableNum(VariableSpace space, const char* name, int defaultval, int faultval, bool allowtrail)
{
    const char* val = NULL;

    val = GetVariable(space, name);
    return ParseVariableNum(val, defaultval, faultval, allowtrail);
}

void PrintVariables(VariableSpace space)
{
    struct _variable* ptr = NULL;

    if (space == NULL)
        return;

    for (ptr = space->next; ptr != NULL; ptr = ptr->next) {
        if (ptr->value != NULL)
            printf("%s = '%s'\n", ptr->name, ptr->value);
        if (cancel_pressed)
            break;
    }
}

bool SetVariable(VariableSpace space, const char* name, const char* value)
{
    struct _variable *current = NULL, *previous = NULL;

    if (space == NULL) {
        return false;
    }
    if (!valid_variable_name(name)) {
        return false;
    }
    if (value == NULL) {
        return DeleteVariable(space, name);
    }
    for (previous = space, current = space->next; NULL != current; previous = current, current = current->next) {
        if (strcmp(current->name, name) == 0) {
            /* found entry, so update */
            if (current->value != NULL) {
                free(current->value);
            }
            current->value = pg_strdup(value);
            if (current->assign_hook != NULL) {
                (*current->assign_hook)(current->value);
            }
            return true;
        }
    }

    /* not present, make new entry */
    current = (struct _variable*)pg_malloc(sizeof(*current));
    current->name = pg_strdup(name);
    current->value = pg_strdup(value);
    current->assign_hook = NULL;
    current->next = NULL;
    previous->next = current;
    return true;
}

/*
 * This both sets a hook function, and calls it on the current value (if any)
 */
bool SetVariableAssignHook(VariableSpace space, const char* name, VariableAssignHook hook)
{
    struct _variable *current = NULL, *previous = NULL;

    if (space == NULL) {
        return false;
    }
    if (!valid_variable_name(name)) {
        return false;
    }
    for (previous = space, current = space->next; NULL != current; previous = current, current = current->next) {
        if (strcmp(current->name, name) == 0) {
            /* found entry, so update */
            current->assign_hook = hook;
            (*hook)(current->value);
            return true;
        }
    }

    /* not present, make new entry */
    current = (struct _variable*)pg_malloc(sizeof(*current));
    current->name = pg_strdup(name);
    current->value = NULL;
    current->assign_hook = hook;
    current->next = NULL;
    previous->next = current;
    (*hook)(NULL);
    return true;
}

bool SetVariableBool(VariableSpace space, const char* name)
{
    return SetVariable(space, name, "on");
}

bool DeleteVariable(VariableSpace space, const char* name)
{
    struct _variable *current = NULL, *previous = NULL;

    if (space == NULL) {
        return false;
    }
    for (previous = space, current = space->next; current != NULL; previous = current, current = current->next) {
        if (strcmp(current->name, name) == 0) {
            if (current->value != NULL) {
                free(current->value);
            }
            current->value = NULL;
            /* Physically delete only if no hook function to remember */
            if (current->assign_hook != NULL) {
                (*current->assign_hook)(NULL);
            }
            else {
                previous->next = current->next;
                free(current->name);
                current->name = NULL;
                free(current);
                current = NULL;
            }
            return true;
        }
    }

    return true;
}

/* Try to interpret value as integer. If value is not integer, retrun 0 */
int ParseVariableInt(const char* val)
{
    int int_result = 0;

    if (val != NULL) {
        char* end = NULL;

        int_result = (int)strtol(val, &end, 0);

        /* If val is not integer, return 0. */
        if (*end != '\0' || end == val) {
            int_result = 0;
        }
    }
    return int_result;
}

