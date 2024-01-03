#include <fstream>
#include <sstream>
#include <unordered_set>
#include <vector>
#include <algorithm>
#include "progress_bar.hpp" // Include the ProgressBar class
#include "utils.hpp"

// Function to process files
std::vector<int> get_vantage_points(const std::vector<std::string>& file_paths) {
    std::unordered_set<int> vantage_points_set;
    ProgressBar bar(file_paths.size());

    std::ifstream first_file(file_paths.front());
    std::string first_line;
    std::getline(first_file, first_line);
    first_file.close();

    int as_path_index = findColumnIndex(first_line, "as_path"); // Throws if column not found

    for (const auto& file_path : file_paths) {
        std::ifstream file(file_path);
        if (!file.is_open()) {
            throw std::runtime_error("Error opening file");
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
            vantage_points_set.insert(vantage_point);
        }

        file.close();
        bar.update();
    }

    bar.close();

    // Convert set to vector and sort
    std::vector<int> vantage_points(vantage_points_set.begin(), vantage_points_set.end());
    std::sort(vantage_points.begin(), vantage_points.end());

    return vantage_points;
}
