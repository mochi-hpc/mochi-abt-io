/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

static const int json_type_int64 = json_type_int;

/* TODO: review this and cut unused macros */

// Can be used in configurations to check if a JSON object has a particular
// field. If it does, the __out parameter is set to that field.
#define CONFIG_HAS(__config, __key, __out) \
    ((__out = json_object_object_get(__config, __key)) != NULL)

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
            else if (json_object_get_int64(_tmp) != __value)                  \
                fprintf(stderr, "Overriding field \"%s\" (%d) with value %d", \
                        __field_name, json_object_get_int64(_tmp), __value);  \
        }                                                                     \
        json_object_object_add(__config, __key,                               \
                               json_object_new_int64(__value));               \
    } while (0)
