#pragma once

#if defined(_WIN32)
#    if defined(SPARROW_ROCKFINCH_STATIC_LIB)
#        define SPARROW_ROCKFINCH_API
#    elif defined(SPARROW_ROCKFINCH_EXPORTS)
#        define SPARROW_ROCKFINCH_API __declspec(dllexport)
#    else
#        define SPARROW_ROCKFINCH_API __declspec(dllimport)
#    endif
#else
#    define SPARROW_ROCKFINCH_API __attribute__((visibility("default")))
#endif
