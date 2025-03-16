#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
//#include <pybind11/optional.h>
#include "utils.hpp"
#include "get_vantage_points.hpp"
#include "vantage_point_stat.hpp"
#include "get_vantage_point_stat.hpp"

namespace py = pybind11;
#define PYBIND11_DETAILED_ERROR_MESSAGES

PYBIND11_MODULE(mrtc, m) {
    m.def("get_relevant_paths", &get_relevant_paths, py::arg("file_paths"));
    m.def("get_vantage_points", &get_vantage_points, py::arg("file_paths"));
    m.def("get_vantage_point_stat",
          &get_vantage_point_stat,
          py::arg("vantage_point"), py::arg("as_rank"), py::arg("file_paths"), py::arg("get_path_poisoning"));

    // Binding for VantagePointStat class
    py::class_<VantagePointStat>(m, "VantagePointStat")
        .def(py::init<int, int>())
        .def("add_ann", &VantagePointStat::add_ann)
        .def("__lt__", &VantagePointStat::operator<)
        .def_readonly("asn", &VantagePointStat::asn)
        .def_readonly("as_rank", &VantagePointStat::as_rank)
        .def_readonly("ann_count", &VantagePointStat::ann_count)
        .def_readonly("prefix_id_set", &VantagePointStat::prefix_id_set)
        .def_readonly("no_path_poisoning_prefix_id_set", &VantagePointStat::no_path_poisoning_prefix_id_set);
}
