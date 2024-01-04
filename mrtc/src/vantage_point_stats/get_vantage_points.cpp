#include <fstream>
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include "progress_bar.hpp" // Include the ProgressBar class
#include "utils.hpp"

std::unordered_map<int, std::unordered_set<std::string>> get_vantage_points(
    const std::unordered_map<std::string, std::vector<std::string>>& directories) {

    if (directories.empty()) {
        return {};
    }

    // Calculate the total number of files
    int total_files = 0;
    for (const auto& dir_pair : directories) {
        total_files += dir_pair.second.size();
    }

    ProgressBar bar(total_files, "Getting vantage points: ");

    // Find as_path_index from the first file of the first directory
    auto first_file_iter = directories.begin()->second.begin();
    std::ifstream first_file(*first_file_iter);
    std::string first_line;
    std::getline(first_file, first_line);
    first_file.close();

    int as_path_index = findColumnIndex(first_line, "as_path"); // Throws if column not found

    std::unordered_map<int, std::unordered_set<std::string>> vantage_points_directories;

    for (const auto& dir_pair : directories) {
        const std::string& directory = dir_pair.first;
        const std::vector<std::string>& file_paths = dir_pair.second;

        if (file_paths.empty()) {
            continue; // Skip empty directories
        }

        for (const auto& file_path : file_paths) {
            std::ifstream file(file_path);
            if (!file.is_open()) {
                throw std::runtime_error("Error opening file: " + file_path);
            }

            std::string line;
            std::getline(file, line); // Skip the header line

            while (std::getline(file, line)) {
                std::stringstream ss(line);
                std::string cell;
                int current_index = 0;
                std::string as_path_cell;

                while (std::getline(ss, cell, '\t')) {
                    if (current_index == as_path_index) {
                        as_path_cell = cell;
                        break;
                    }
                    current_index++;
                }

                if (as_path_cell.empty() || as_path_cell.find("}") != std::string::npos) {
                    continue;
                }

                std::stringstream as_path_stream(as_path_cell);
                int vantage_point;
                as_path_stream >> vantage_point;

                // Add the directory to the corresponding vantage point
                vantage_points_directories[vantage_point].insert(directory);
            }

            file.close();
            bar.update();
        }
    }

    bar.close();
    return vantage_points_directories;
}
