#include "progress_bar.hpp"
#include <iostream>
#include <iomanip>
#include <sstream>

ProgressBar::ProgressBar(int total) : total(total), completed(0) {
    start_time = std::chrono::steady_clock::now();
}

void ProgressBar::update(int update_total) {
    completed = completed + update_total;
    // Don't add overhead if we're going to be doing this a lot
    if (completed % 100 == 0 or total < 1000){
        auto now = std::chrono::steady_clock::now();
        std::chrono::seconds elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start_time);

        // Estimate remaining time
        double rate = completed / (double)elapsed.count();
        int remaining_time = static_cast<int>((total - completed) / rate);
        std::chrono::seconds remaining_duration(remaining_time);

        // Display the progress
        std::cout << "\r" // Move to the start of the line
                  << "Completed: " << completed << "/" << total
                  << " [" << formatDuration(elapsed) << " elapsed, "
                  << formatDuration(remaining_duration) << " remaining]"
                  << std::flush;
    }
}

void ProgressBar::set_total_completed(int total_completed) {
    completed = total_completed;
    update(0);
}

std::string ProgressBar::formatDuration(std::chrono::seconds duration) {
    int hours = duration.count() / 3600;
    int minutes = (duration.count() % 3600) / 60;
    int seconds = duration.count() % 60;
    std::ostringstream formatted;
    formatted << std::setfill('0') << std::setw(2) << hours << ":"
              << std::setfill('0') << std::setw(2) << minutes << ":"
              << std::setfill('0') << std::setw(2) << seconds;
    return formatted.str();
}

void ProgressBar::close(){
    std::cout << std::endl; // New line after ProgressBar completes
}
