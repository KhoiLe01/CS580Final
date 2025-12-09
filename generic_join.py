import csv
import time
from pathlib import Path
from typing import Dict, List, Tuple, Set


# ---------------------------------------------------------------
# SCHEMA
# ---------------------------------------------------------------
SCHEMAS = {
    "R1": ["A1", "A2"],
    "R2": ["A2", "A3"],
    "R3": ["A1", "A3"],
    "R4": ["A3", "A4"],
    "R5": ["A4", "A5"],
    "R6": ["A5", "A6"],
    "R7": ["A4", "A6"],
}

ATTR_ORDER = ["A1", "A2", "A3", "A4", "A5", "A6"]


# ---------------------------------------------------------------
# LOAD RELATIONS
# ---------------------------------------------------------------
def load_relations(dir_path: str):
    base = Path(dir_path)
    relations = {}

    for rname, schema in SCHEMAS.items():
        filename = base / f"{rname}.csv"
        rows = []
        with open(filename, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(tuple(int(row[attr]) for attr in schema))
        relations[rname] = rows

    return relations


# ---------------------------------------------------------------
# INDEX CONSTRUCTION
# ---------------------------------------------------------------
def build_indexes(relations):
    proj_all = {}
    index = {}

    for rname, schema in SCHEMAS.items():
        X, Y = schema
        proj_all[(rname, X)] = set()
        proj_all[(rname, Y)] = set()

        XY_map = {}
        YX_map = {}

        for x, y in relations[rname]:
            proj_all[(rname, X)].add(x)
            proj_all[(rname, Y)].add(y)
            XY_map.setdefault(x, set()).add(y)
            YX_map.setdefault(y, set()).add(x)

        index[(rname, X)] = XY_map
        index[(rname, Y)] = YX_map

    return proj_all, index


# ---------------------------------------------------------------
# GENERIC JOIN CORE
# ---------------------------------------------------------------
def generic_join(relations):
    proj_all, index = build_indexes(relations)
    results = []

    def get_allowed(var, prefix):
        candidate_sets = []

        for rname, schema in SCHEMAS.items():
            if var not in schema:
                continue
            other = [a for a in schema if a != var][0]

            if other not in prefix:
                candidate_sets.append(proj_all[(rname, var)])
            else:
                ov = prefix[other]
                candidate_sets.append(index[(rname, other)].get(ov, set()))

        if not candidate_sets:
            return []

        candidate_sets.sort(key=len)
        values = set(candidate_sets[0])
        for s in candidate_sets[1:]:
            values &= s
            if not values:
                break
        return sorted(values)

    def recurse(i, prefix):
        if i == len(ATTR_ORDER):
            results.append(tuple(prefix[a] for a in ATTR_ORDER))
            return
        var = ATTR_ORDER[i]
        for v in get_allowed(var, prefix):
            prefix[var] = v
            recurse(i + 1, prefix)
            del prefix[var]

    recurse(0, {})
    return results

def generic_join_subquery(vars_in_order, edges, relations):
    """
    vars_in_order: list of variables for this subquery (bag.vars)
    edges: list of (rel_name, [attrs]) pairs
    relations: dict: rel_name -> list of tuples/dicts or raw tuples
    """

    # Build a local schema dictionary
    local_schemas = {rel: attrs for rel, attrs in edges}

    # Build projection information and indexes
    proj_all = {}
    index = {}

    # Normalize relations into row dicts
    normalized = {}
    for rel, attrs in edges:
        normalized_rows = []
        for tup in relations[rel]:
            # Relation may be tuple or dict
            if isinstance(tup, dict):
                normalized_rows.append({a: tup[a] for a in attrs})
            else:
                normalized_rows.append({a: v for a, v in zip(attrs, tup)})
        normalized[rel] = normalized_rows

        # Build projection sets
        for a in attrs:
            proj_all[(rel, a)] = set()

    # Build indexes
    for rel, attrs in edges:
        rows = normalized[rel]
        if len(attrs) == 1:
            # unary case
            a = attrs[0]
            for row in rows:
                proj_all[(rel, a)].add(row[a])
            continue

        # binary or higher (our bags are binary)
        a, b = attrs
        a_map = {}
        b_map = {}
        for row in rows:
            av = row[a]
            bv = row[b]
            proj_all[(rel, a)].add(av)
            proj_all[(rel, b)].add(bv)
            a_map.setdefault(av, set()).add(bv)
            b_map.setdefault(bv, set()).add(av)

        index[(rel, a)] = a_map
        index[(rel, b)] = b_map

    # Recursive enumeration (copied from your main generic_join, but adapted)
    results = []

    def get_allowed(var, prefix):
        candidate_sets = []

        # Look only at relations involving this var in this bag
        for rel, attrs in edges:
            if var not in attrs:
                continue
            # Find the "other" attribute (bags are binary)
            other_attrs = [a for a in attrs if a != var]
            if not other_attrs:
                # unary relation
                candidate_sets.append(proj_all[(rel, var)])
                continue

            other = other_attrs[0]
            if other not in prefix:
                candidate_sets.append(proj_all[(rel, var)])
            else:
                ov = prefix[other]
                s = index[(rel, other)].get(ov, set())
                candidate_sets.append(s)

        if not candidate_sets:
            return []

        candidate_sets.sort(key=len)
        vals = set(candidate_sets[0])
        for s in candidate_sets[1:]:
            vals &= s
            if not vals:
                break
        return sorted(vals)

    def recurse(i, prefix):
        if i == len(vars_in_order):
            # Emit a row
            results.append(prefix.copy())
            return
        var = vars_in_order[i]
        for v in get_allowed(var, prefix):
            prefix[var] = v
            recurse(i + 1, prefix)
            del prefix[var]

    recurse(0, {})
    return results



# ---------------------------------------------------------------
# TIMING FUNCTIONS FOR EXPERIMENTS
# ---------------------------------------------------------------
def run_genericjoin(dirpath):
    relations = load_relations(dirpath)
    return generic_join(relations)


def time_genericjoin(dirpath):
    start = time.time()
    results = run_genericjoin(dirpath)
    end = time.time()
    return end - start, len(results)


# ---------------------------------------------------------------
# MANUAL TESTING
# ---------------------------------------------------------------
if __name__ == "__main__":
    t, size = time_genericjoin("query_relations")
    print(f"Runtime: {t:.4f} sec")
    print(f"Output size: {size}")
