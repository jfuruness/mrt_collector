#ifndef UTILS_HPP
#define UTILS_HPP

#include <string>

int findColumnIndex(const std::string& header_line, const std::string& column_name);

std::vector<std::string> get_relevant_paths(const std::vector<std::string>& file_paths);

#endif
