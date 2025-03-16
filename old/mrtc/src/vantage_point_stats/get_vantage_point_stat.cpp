#include "vantage_point_stat.hpp"
#include "utils.hpp" // Include the utility function
#include "progress_bar.hpp"
#include <fstream>
#include <sstream>
#include <vector>
#include <string>

VantagePointStat get_vantage_point_stat(int vantage_point, int as_rank, const std::vector<std::string>& file_paths, bool get_path_poisoning) {
    if (file_paths.empty()) {
        throw std::runtime_error("No file paths provided.");
    }

    //ProgressBar bar(file_paths.size());

    // Open the first file to read the header and find column indices
    std::ifstream first_file(file_paths.front());
    std::string header_line;
    std::getline(first_file, header_line);
    first_file.close();

    int as_path_index = findColumnIndex(header_line, "as_path");
    int invalid_as_path_asns_index = findColumnIndex(header_line, "invalid_as_path_asns");
    // int ixps_in_as_path_index = findColumnIndex(header_line, "ixps_in_as_path");
    int prepending_index = findColumnIndex(header_line, "prepending");
    int as_path_loop_index = findColumnIndex(header_line, "as_path_loop");
    int input_clique_split_index = findColumnIndex(header_line, "input_clique_split");
    int prefix_id_index = findColumnIndex(header_line, "prefix_id");

    VantagePointStat stat(vantage_point, as_rank);

    for (const auto& file_path : file_paths) {
        std::ifstream file(file_path);
        std::string line;

        // Skip the first line (header)
        std::getline(file, line);

        while (std::getline(file, line)) {
            std::stringstream ss(line);
            std::string cell;
            std::vector<std::string> row;

            while (std::getline(ss, cell, '\t')) {
                row.push_back(cell);
            }

            // Check for '}' in as_path and skip if present
            std::string as_path = row[as_path_index];
            if (as_path.find("}") != std::string::npos) {
                continue;
            }

            // Split as_path and get the first ASN
            std::stringstream as_path_stream(as_path);
            std::string asn_str;
            std::getline(as_path_stream, asn_str, ' ');
            int current_vantage_point = std::stoi(asn_str);
            if (current_vantage_point != vantage_point) {
                continue;
            }

            int prefix_id = std::stoi(row[prefix_id_index]);
            if (get_path_poisoning){
                // Simplified path poisoning logic
                bool path_poisoning = !(row[invalid_as_path_asns_index] == "[]" &&
                                        row[prepending_index] == "False" &&
                                        row[as_path_loop_index] == "False" &&
                                        row[input_clique_split_index] == "False");
                stat.add_ann(prefix_id, path_poisoning);
            } else {
                stat.add_ann(prefix_id, true);
            }
        }
        //bar.update();
    }
    //bar.close();
    return stat;
}
