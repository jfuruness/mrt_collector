#include "vantage_point_stat.hpp"

VantagePointStat::VantagePointStat(int asn, int as_rank) : asn(asn), ann_count(0), as_rank(as_rank) {
    prefix_id_set.reserve(800000);
    no_path_poisoning_prefix_id_set.reserve(800000);
}

void VantagePointStat::add_ann(int prefix_id, bool path_poisoning) {
    prefix_id_set.insert(prefix_id);
    ++ann_count;
    if (!path_poisoning) {
        no_path_poisoning_prefix_id_set.insert(prefix_id);
    }
}

bool VantagePointStat::operator<(const VantagePointStat& other) const {
    if (as_rank != other.as_rank) {
        return as_rank < other.as_rank;
    }
    if (ann_count != other.ann_count) {
        return ann_count > other.ann_count;
    }
    return asn < other.asn;
}
