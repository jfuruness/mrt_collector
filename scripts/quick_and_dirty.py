from collections import defaultdict
import csv

from lib_roa_checker import ROAValidity

from tqdm import tqdm


class ReportGenerator:
    def __init__(self, path):
        self.path = path
        self.stats = {"lr_cb_roa": 0,
                      "lr_invalid_roa": 0,
                      "lr_hijack": 0,
                      "lr_hijack_detected_cb_roa": 0,
                      "lr_hijack_detected_invalid_by_roa": 0,
                      "lr_prefixes_that_are_subprefixes_of_valid_prefixes": 0,
                      "lr_prefixes_that_are_subprefixes_of_prefixes_covered_by_roa": 0,
                      "lr_prefixes_with_invalid_subprefix": 0,
                      "lr_valid_prefixes_with_invalid_subprefix": 0,
                      "lr_invalid_subprefix_cb_prefix": 0,
                      "lr_invalid_hijack_subprefix_cb_prefix": 0,
                      "total_rows": 0,
                      }


        # LR = local_rib
        # CB = covered_by


        # Local RIB covered by a roa with an ASN of 0 (these are all invalid)
        # Invalid by roa subprefixes that are contained within other prefixes and bgpstream hijack

    def run(self):
        from lib_cidr_trie import IPv4CIDRTrie, IPv6CIDRTrie
        from ipaddress import ip_network
        from tqdm import trange

        with open(self.path, "r") as f:
            reader = csv.DictReader(f, delimiter="\t")
            rows = list(reader)
            prefixes = set([ip_network(x["prefix"]) for x in rows])

            def get_possible_superprefixes(p):
                return [p.supernet(new_prefix=i) for i in range(p.prefixlen, -1, -1)]

            prefix_superprefix_dict = dict()
            for prefix in tqdm(prefixes, total=len(prefixes), desc="superprefixes"):
                prefix_superprefix_dict[prefix] = []
                for possible_superprefix in get_possible_superprefixes(prefix):
                    if possible_superprefix in prefixes:
                        prefix_superprefix_dict[prefix].append(possible_superprefix)

            prefix_subprefix_dict = defaultdict(list)
            for prefix, superprefixes in tqdm(prefix_superprefix_dict.items(),
                                              total=len(prefix_superprefix_dict),
                                              desc="subprefixes"):
                for superprefix in superprefixes:
                    prefix_subprefix_dict[superprefix].append(prefix)




            prefix_row_dict = defaultdict(list)
            for row in tqdm(rows, total=len(rows), desc="building first dict"):
                prefix_row_dict[ip_network(row["prefix"])].append(row)

            for row in tqdm(rows, total=len(rows)):
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

                row_prefix = ip_network(row["prefix"])

                lr_prefixes_that_are_subprefixes_of_valid_prefixes = False #
                lr_prefixes_that_are_subprefixes_of_prefixes_covered_by_roa = False #
                lr_prefixes_with_invalid_subprefix = False #
                lr_valid_prefixes_with_invalid_subprefix = False  #
                lr_invalid_subprefix_cb_prefix = False  #
                lr_invalid_hijack_subprefix_cb_prefix = False  #


                superprefixes = prefix_superprefix_dict.get(row_prefix, [])
                subprefixes = prefix_subprefix_dict.get(row_prefix, [])

                superprefix_rows = []
                for x in superprefixes:
                    superprefix_rows.extend(prefix_row_dict[x])
                subprefix_rows = []
                for x in subprefixes:
                    subprefix_rows.extend(prefix_row_dict[x])

                if len(subprefix_rows) > 0:
                    for subprefix_row in subprefix_rows:
                        if subprefix_row["roa_validity"] == str(ROAValidity.INVALID.value):
                            lr_prefixes_with_invalid_subprefix = True
                            if row["roa_validity"] == str(ROAValidity.VALID.value):
                                lr_valid_prefixes_with_invalid_subprefix = True

                if len(superprefix_rows) > 0:
                    for superprefix_row in superprefix_rows:
                        if superprefix_row["roa_validity"] == str(ROAValidity.VALID.value):
                            lr_prefixes_that_are_subprefixes_of_valid_prefixes = True
                        if superprefix_row["roa_validity"] != str(ROAValidity.UNKNOWN.value):
                            lr_prefixes_that_are_subprefixes_of_prefixes_covered_by_roa = True
                    if row["roa_validity"] == str(ROAValidity.INVALID.value):
                        lr_invalid_subprefix_cb_prefix = True
                        if row["hijack_detected_roa_validity"] not in [None, "", "None"]:
                            lr_invalid_hijack_subprefix_cb_prefix = True



                self.stats["lr_prefixes_that_are_subprefixes_of_valid_prefixes"] += int(lr_prefixes_that_are_subprefixes_of_valid_prefixes)
                self.stats["lr_prefixes_that_are_subprefixes_of_prefixes_covered_by_roa"] += int(lr_prefixes_that_are_subprefixes_of_prefixes_covered_by_roa)
                self.stats["lr_prefixes_with_invalid_subprefix"] += int(lr_prefixes_with_invalid_subprefix)
                self.stats["lr_valid_prefixes_with_invalid_subprefix"] += int(lr_valid_prefixes_with_invalid_subprefix)
                self.stats["lr_invalid_subprefix_cb_prefix"] += int(lr_invalid_subprefix_cb_prefix)
                self.stats["lr_invalid_hijack_subprefix_cb_prefix"] += int(lr_invalid_hijack_subprefix_cb_prefix)

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
    for i, fname in enumerate(os.listdir("/data/mrt_cache")):
        if "local" not in fname and "results_files" not in fname:
            stats[fname] = ReportGenerator(f"/data/mrt_cache/{fname}/parsed.tsv").run()
            print(i)
            print(stats)

    import yaml
    print(yaml.dump(stats))
