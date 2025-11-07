#pragma once

#if defined(_WIN32)
#    if defined(SPARROW_PYCAPSULE_STATIC_LIB)
#        define SPARROW_PYCAPSULE_API
#    elif defined(SPARROW_PYCAPSULE_EXPORTS)
#        define SPARROW_PYCAPSULE_API __declspec(dllexport)
#    else
#        define SPARROW_PYCAPSULE_API __declspec(dllimport)
#    endif
#else
#    define SPARROW_PYCAPSULE_API __attribute__((visibility("default")))
#endif
