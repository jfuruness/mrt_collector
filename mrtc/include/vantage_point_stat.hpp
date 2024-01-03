#ifndef VANTAGEPOINTSTATS_HPP
#define VANTAGEPOINTSTATS_HPP

#include <set>
#include <string>
#include <vector>
#include <unordered_map>

class VantagePointStat {

public:
    int asn;
    std::set<int> prefix_id_set;
    std::set<int> no_path_poisoning_prefix_id_set;
    int ann_count;
    int as_rank;

    VantagePointStat(int asn, int as_rank = 500000);

    void add_ann(int prefix_id, bool path_poisoning);
    bool operator<(const VantagePointStat& other) const;

};

#endif // VANTAGEPOINTSTATS_HPP
