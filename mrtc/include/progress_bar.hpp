#ifndef PROGRESSBAR_HPP
#define PROGRESSBAR_HPP

#include <chrono>
#include <string>

class ProgressBar {
private:
    int total;
    int completed;
    const std::string desc;
    std::chrono::time_point<std::chrono::steady_clock> start_time;

    std::string formatDuration(std::chrono::seconds duration);

public:
    ProgressBar(int total, const std::string& desc = "Completed: ");
    void update(int update_total=1);
    void set_total_completed(int total_completed);
    void close();
};

#endif // PROGRESSBAR_HPP
