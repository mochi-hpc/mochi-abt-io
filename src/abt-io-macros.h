/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef __ABT_IO_MACROS
#define __ABT_IO_MACROS

static const int json_type_int64 = json_type_int;

// Can be used in configurations to check if a JSON object has a particular
// field. If it does, the __out parameter is set to that field.
#define CONFIG_HAS(__config, __key, __out) \
    ((__out = json_object_object_get(__config, __key)) != NULL)

// Checks if a JSON object has a particular key and its value is of type object.
// If the field does not exist, creates it with an empty object.
// If the field exists but is not of type object, prints an error and return -1.
// After a call to this macro, __out is set to the ceated/found field.
#define CONFIG_HAS_OR_CREATE_OBJECT(__config, __key, __fullname, __out)        \
    do {                                                                       \
        __out = json_object_object_get(__config, __key);                       \
        if (__out && !json_object_is_type(__out, json_type_object)) {          \
            fprintf(stderr, "\"%s\" is in configuration but is not an object", \
                    __fullname);                                               \
            return -1;                                                         \
        }                                                                      \
        if (!__out) {                                                          \
            __out = json_object_new_object();                                  \
            json_object_object_add(__config, __key, __out);                    \
        }                                                                      \
    } while (0)

// Overrides a field with a boolean. If the field already existed and was
// different from the new value, and __warning is true, prints a warning.
#define CONFIG_OVERRIDE_BOOL(__config, __key, __value, __field_name,          \
                             __warning)                                       \
    do {                                                                      \
        struct json_object* _tmp = json_object_object_get(__config, __key);   \
        if (_tmp && __warning) {                                              \
            if (!json_object_is_type(_tmp, json_type_boolean))                \
                fprintf(stderr, "Overriding field \"%s\" with value \"%s\"",  \
                        __field_name, __value ? "true" : "false");            \
            else if (json_object_get_boolean(_tmp) != !!__value)              \
                fprintf(stderr,                                               \
                        "Overriding field \"%s\" (\"%s\") with value \"%s\"", \
                        __field_name,                                         \
                        json_object_get_boolean(_tmp) ? "true" : "false",     \
                        __value ? "true" : "false");                          \
        }                                                                     \
        json_object_object_add(__config, __key,                               \
                               json_object_new_boolean(__value));             \
    } while (0)

// Overrides a field with an integer. If the field already existed and was
// different from the new value, and __warning is true, prints a warning.
#define CONFIG_OVERRIDE_INTEGER(__config, __key, __value, __field_name,       \
                                __warning)                                    \
    do {                                                                      \
        struct json_object* _tmp = json_object_object_get(__config, __key);   \
        if (_tmp && __warning) {                                              \
            if (!json_object_is_type(_tmp, json_type_int))                    \
                fprintf(stderr, "Overriding field \"%s\" with value %d",      \
                        __field_name, (int)__value);                          \
            else if (json_object_get_int(_tmp) != __value)                    \
                fprintf(stderr, "Overriding field \"%s\" (%d) with value %d", \
                        __field_name, json_object_get_int(_tmp), __value);    \
        }                                                                     \
        json_object_object_add(__config, __key,                               \
                               json_object_new_int64(__value));               \
    } while (0)

// Overrides a field with a string. If the field already existed and was
// different from the new value, and __warning is true, prints a warning.
#define CONFIG_OVERRIDE_STRING(__config, __key, __value, __fullname,          \
                               __warning)                                     \
    do {                                                                      \
        struct json_object* _tmp = json_object_object_get(__config, __key);   \
        if (_tmp && __warning) {                                              \
            if (!json_object_is_type(_tmp, json_type_string))                 \
                fprintf(stderr, "Overriding field \"%s\" with value \"%s\"",  \
                        __fullname, __value);                                 \
            else if (strcmp(json_object_get_string(_tmp), __value) != 0)      \
                fprintf(stderr,                                               \
                        "Overriding field \"%s\" (\"%s\") with value \"%s\"", \
                        __fullname, json_object_get_string(_tmp), __value);   \
        }                                                                     \
        _tmp = json_object_new_string(__value);                               \
        json_object_object_add(__config, __key, _tmp);                        \
    } while (0)

#endif /* __ABT_IO_MACROS */
