#include <fstream>
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include "progress_bar.hpp" // Include the ProgressBar class
#include "utils.hpp"

// Function to process files
std::unordered_map<int, std::unordered_set<std::string>> get_vantage_points(
    const std::unordered_map<std::string, std::vector<std::string>>& directories);
