#ifndef NPY_NO_DEPRECATED_API

/* put this check here since there may be multiple includes in C extensions. */
#if defined(NDARRAYTYPES_H) || defined(_NPY_DEPRECATED_API_H) || \
    defined(OLD_DEFINES_H)
#error "npy_no_deprecated_api.h" must be first among numpy includes.
#else
#define NPY_NO_DEPRECATED_API NPY_API_VERSION
#endif

#endif