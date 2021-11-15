from collections import defaultdict
import csv

from lib_roa_checker import ROAValidity


class ReportGenerator:
    def __init__(self, path):
        self.path = path
        self.stats = {"lr_cb_roa": 0,
                      "lr_invalid_roa": 0,
                      "lr_hijack": 0,
                      "lr_hijack_detected_cb_roa": 0,
                      "lr_hijack_detected_invalid_by_roa": 0,
                      "total_rows": 0}


        # LR = local_rib
        # CB = covered_by


        # STILL NEED TODO
        # NOTE: USE YOUR CIDR TRIE FOR ALL OF THESE!!
        #   I think this literally allows for the in syntax
        # prefixes in local rib which are a subprefix of prefixes that are covered by a roa
        # number of prefixes that that have an invalid subprefix
        # Number of valid prefixes that have an invalid subprefix
        # Local RIB covered by a roa with an ASN of 0 (these are all invalid)
        # Invalid by roa subprefixes that are contained within other prefixes
        # Invalid by roa subprefixes that are contained within other prefixes and bgpstream hijack

    def run(self):
        with open(self.path, "r") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                # Local rib covered by roa
                if int(row["roa_validity"]) != ROAValidity.UNKNOWN.value:
                    self.stats["lr_cb_roa"] = self.stats["lr_cb_roa"] + 1
                # Local RIB invalid ROA
                if int(row["roa_validity"]) == ROAValidity.INVALID.value:
                    self.stats["lr_invalid_roa"] = self.stats["lr_invalid_roa"] + 1
                # Local RIB hijack
                if row["hijack_expected_origin_number"] not in ["", None]:
                    self.stats["lr_hijack"] = self.stats["lr_hijack"] + 1

                # Local RIB hijack and covered by a ROA
                if row["hijack_detected_roa_validity"] not in [str(ROAValidity.UNKNOWN.value), None, ""]:
                    self.stats["lr_hijack_detected_cb_roa"] = self.stats["lr_hijack_detected_cb_roa"] + 1

                # Local RIB hijack and invalid by ROA
                if row["hijack_detected_roa_validity"] == str(ROAValidity.INVALID.value):
                    self.stats["lr_hijack_detected_invalid_by_roa"] = self.stats["lr_hijack_detected_invalid_by_roa"] + 1

                self.stats["total_rows"] = self.stats["total_rows"] + 1

        temp_stats = {k.replace("lr", "local_rib").replace("cb", "covered_by"): v
                      for k, v in self.stats.items()}
        print(temp_stats)
        return temp_stats

if __name__ == "__main__":
    # Loop over dates
    # Save the date: temp_stats to YAML
    stats = {}
    import os
    for fname in os.listdir("/data/mrt_cache"):
        if "local" not in fname:
            stats[fname] = ReportGenerator(f"/data/mrt_cache/{fname}/parsed.tsv").run()

    import yaml
    print(yaml.dump(stats))
