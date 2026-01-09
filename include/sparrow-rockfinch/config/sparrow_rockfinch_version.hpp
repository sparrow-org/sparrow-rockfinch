#pragma once

#include <string>

namespace sparrow::rockfinch
{
    constexpr int SPARROW_ROCKFINCH_VERSION_MAJOR = 0;
    constexpr int SPARROW_ROCKFINCH_VERSION_MINOR = 1;
    constexpr int SPARROW_ROCKFINCH_VERSION_PATCH = 0;

    constexpr int SPARROW_ROCKFINCH_BINARY_CURRENT = 1;
    constexpr int SPARROW_ROCKFINCH_BINARY_REVISION = 0;
    constexpr int SPARROW_ROCKFINCH_BINARY_AGE = 0;


    // Generate version string
    static const std::string SPARROW_ROCKFINCH_VERSION_STRING = std::to_string(SPARROW_ROCKFINCH_VERSION_MAJOR)
                                                                + "."
                                                                + std::to_string(SPARROW_ROCKFINCH_VERSION_MINOR)
                                                                + "."
                                                                + std::to_string(SPARROW_ROCKFINCH_VERSION_PATCH);

    static_assert(
        SPARROW_ROCKFINCH_BINARY_AGE <= SPARROW_ROCKFINCH_BINARY_CURRENT,
        "SPARROW_ROCKFINCH_BINARY_AGE cannot be greater than SPARROW_ROCKFINCH_BINARY_CURRENT"
    );
}
