# Mostly used from: https://github.com/pybind/scikit_build_example
# basically the only example I could find using cmake from a pip install
# (rather than others which required a git clone)


# Require CMake 3.15+ (matching scikit-build-core) Use new versions of all
# policies up to CMake 3.27
cmake_minimum_required(VERSION 3.15...3.27)

# Scikit-build-core sets these values for you, or you can just hard-code the
# name and version.
project(
  ${SKBUILD_PROJECT_NAME}
  VERSION ${SKBUILD_PROJECT_VERSION}
  LANGUAGES CXX)

### Personal modifications##############################
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Set the default build type to Release if not specified
#if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
#endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native")


# TODO
# NOTE: tried it, seems to make only about 1s worth of difference
# but I'll keep it just in case
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-generate")

# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-use")

################################################################

# Find the module development requirements (requires FindPython from 3.17 or
# scikit-build-core's built-in backport)
find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)
find_package(pybind11 CONFIG REQUIRED)

##################modification
# Check for LTO support and enable it if possible
include(CheckIPOSupported)
check_ipo_supported(RESULT lto_supported)
if(lto_supported)
  message(STATUS "LTO supported")
else()
  message(STATUS "LTO not supported")
endif()
#####################################


# Add a library using FindPython's tooling (pybind11 also provides a helper like
# this)
############################################################################

set(MRTC_SOURCES
    mrtc/src/utils.cpp
    mrtc/src/progress_bar.cpp
    mrtc/src/vantage_point_stats/get_vantage_points.cpp
    mrtc/src/vantage_point_stats/vantage_point_stat.cpp
    mrtc/src/main.cpp
)

python_add_library(mrtc MODULE ${MRTC_SOURCES} WITH_SOABI)
##############################################################################

########################modification
if(lto_supported)
  set_property(TARGET mrtc PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()
####################################

# Add the directory where the header files are located
# Going back to og

# Manually include directories
target_include_directories(mrtc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/mrtc/include
    ${CMAKE_CURRENT_SOURCE_DIR}/mrtc/include/vantage_point_stats

)
######################################################################

target_link_libraries(mrtc PRIVATE pybind11::headers)

# This is passing in the version as a define just as an example
target_compile_definitions(mrtc PRIVATE VERSION_INFO=${PROJECT_VERSION})

# The install directory is the output (wheel) directory
install(TARGETS mrtc DESTINATION mrt_collector)
