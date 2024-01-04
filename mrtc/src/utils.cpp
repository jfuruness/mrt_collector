#include <stdexcept>
#include <sstream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>

#include "utils.hpp"
#include "progress_bar.hpp"

// Function to find the index of a specified column in the header
int findColumnIndex(const std::string& header_line, const std::string& column_name) {
    std::stringstream header_stream(header_line);
    std::string header_cell;
    int index = 0;
    while (std::getline(header_stream, header_cell, '\t')) {
        if (header_cell == column_name) {
            return index;
        }
        index++;
    }

    throw std::runtime_error("Column '" + column_name + "' not found in the header.");
}

std::vector<std::string> get_relevant_paths(const std::vector<std::string>& file_paths) {
    std::vector<std::string> relevant_paths;
    ProgressBar pbar(file_paths.size());

    for (const auto& path : file_paths) {
        std::ifstream file(path);
        if (!file.is_open()) {
            pbar.update();
            continue; // Skip if the file cannot be opened
        }

        std::string line;
        getline(file, line); // Read the first line (possibly a header)

        if (getline(file, line)) { // Check if there is a second line
            relevant_paths.push_back(path); // If so, add the path to the relevant paths
        }
        pbar.update();
    }
    pbar.close();
    return relevant_paths;
}

std::unordered_map<std::string, std::vector<std::string>> get_relevant_paths(
    const std::unordered_map<std::string, std::vector<std::string>>& directories) {

    std::unordered_map<std::string, std::vector<std::string>> relevant_paths;

    int total_files = 0;
    for (const auto& dir_pair : directories) {
        total_files += dir_pair.second.size();
    }

    ProgressBar pbar(total_files);

    for (const auto& dir_pair : directories) {
        const std::string& directory = dir_pair.first;
        const std::vector<std::string>& file_paths = dir_pair.second;

        std::vector<std::string> relevant_files_in_dir;

        for (const auto& path : file_paths) {
            std::ifstream file(path);
            if (!file.is_open()) {
                pbar.update();
                continue; // Skip if the file cannot be opened
            }

            std::string line;
            getline(file, line); // Read the first line (possibly a header)

            if (getline(file, line)) { // Check if there is a second line
                relevant_files_in_dir.push_back(path); // If so, add the path to the relevant paths
            }
            pbar.update();
        }

        relevant_paths[directory] = relevant_files_in_dir;
    }
    pbar.close();
    return relevant_paths;
}
