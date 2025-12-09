import csv
import time
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Set, Optional


# ===============================================================
#  SCHEMA FOR THE QUERY q(A1,...,A6)
# ===============================================================
SCHEMAS: Dict[str, List[str]] = {
    "R1": ["A1", "A2"],
    "R2": ["A2", "A3"],
    "R3": ["A1", "A3"],
    "R4": ["A3", "A4"],
    "R5": ["A4", "A5"],
    "R6": ["A5", "A6"],
    "R7": ["A4", "A6"],
}

ATTR_ORDER = ["A1", "A2", "A3", "A4", "A5", "A6"]


# ===============================================================
#  LOAD RELATIONS
# ===============================================================
def load_relations(dir_path: str) -> Dict[str, List[Tuple[int, ...]]]:
    """
    Loads relations R1..R7 as lists of tuples (ints) from CSV files.
    Each CSV must have headers exactly matching SCHEMAS[rname].
    """
    base = Path(dir_path)
    relations: Dict[str, List[Tuple[int, ...]]] = {}

    for rname, schema in SCHEMAS.items():
        filename = base / f"{rname}.csv"
        if not filename.exists():
            raise FileNotFoundError(f"Missing file: {filename}")

        rows: List[Tuple[int, ...]] = []
        with open(filename, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                tup = tuple(int(row[attr]) for attr in schema)
                rows.append(tup)
        relations[rname] = rows

    return relations


# ===============================================================
#  FRACTIONAL HYPERTREE DECOMPOSITION
# ===============================================================
@dataclass
class FBag:
    name: str
    vars: List[str]              # χ(B)
    lambdas: List[str]           # λ(B): relation names in this bag
    parent: Optional[str] = None
    children: List[str] = field(default_factory=list)
    weights: Dict[str, float] = field(default_factory=dict)  # for documentation


def build_fractional_bags() -> Dict[str, FBag]:
    """
    Fixed fractional hypertree decomposition for the 7-relation query.

    Tree:  B2
           |
           B1 - B3 - B4

    Root is B1.
    """
    bags: Dict[str, FBag] = {}

    # Bag B1: χ = {A1,A2,A3}, λ = {R1,R2}
    bags["B1"] = FBag(
        name="B1",
        vars=["A1", "A2", "A3"],
        lambdas=["R1", "R2"],
        parent=None,
        children=["B2", "B3"],
        weights={"R1": 1.0, "R2": 1.0},
    )

    # Bag B2: χ = {A1,A3}, λ = {R3}
    bags["B2"] = FBag(
        name="B2",
        vars=["A1", "A3"],
        lambdas=["R3"],
        parent="B1",
        children=[],
        weights={"R3": 1.0},
    )

    # Bag B3: χ = {A3,A4,A5}, λ = {R4,R5}
    bags["B3"] = FBag(
        name="B3",
        vars=["A3", "A4", "A5"],
        lambdas=["R4", "R5"],
        parent="B1",
        children=["B4"],
        weights={"R4": 1.0, "R5": 1.0},
    )

    # Bag B4: χ = {A4,A5,A6}, λ = {R6,R7}
    bags["B4"] = FBag(
        name="B4",
        vars=["A4", "A5", "A6"],
        lambdas=["R6", "R7"],
        parent="B3",
        children=[],
        weights={"R6": 1.0, "R7": 1.0},
    )

    return bags


# ===============================================================
#  GLOBAL INDEXES (FIX 1)
# ===============================================================
def build_global_indexes(relations: Dict[str, List[Tuple[int, ...]]]):
    """
    Build global projections and adjacency maps once.

    proj_global[(rel, attr)] = set of values of attr in relation rel
    index_global[(rel, attr)] = dict[val] -> set[other_attr_values]
        where 'attr' is one of the two attributes of rel, and
        other_attr_values are the values for the *other* attribute.
    """
    proj_global: Dict[Tuple[str, str], Set[int]] = {}
    index_global: Dict[Tuple[str, str], Dict[int, Set[int]]] = {}

    for rel, schema in SCHEMAS.items():
        a, b = schema

        proj_global[(rel, a)] = set()
        proj_global[(rel, b)] = set()

        amap: Dict[int, Set[int]] = {}
        bmap: Dict[int, Set[int]] = {}

        for tup in relations[rel]:
            av, bv = tup
            proj_global[(rel, a)].add(av)
            proj_global[(rel, b)].add(bv)
            amap.setdefault(av, set()).add(bv)
            bmap.setdefault(bv, set()).add(av)

        index_global[(rel, a)] = amap
        index_global[(rel, b)] = bmap

    return proj_global, index_global


# ===============================================================
#  BAG-LOCAL WORST-CASE OPTIMAL JOIN USING GLOBAL INDEXES
#  (FIX 2 & 3: no rescanning, only lookups & intersections)
# ===============================================================
def bag_generic_join(
    bag: FBag,
    proj_global: Dict[Tuple[str, str], Set[int]],
    index_global: Dict[Tuple[str, str], Dict[int, Set[int]]],
    constraints: Optional[Dict[str, int]] = None,
) -> List[Dict[str, int]]:
    """
    Worst-case optimal join restricted to a single bag.

    - bag.vars : variables in this bag
    - bag.lambdas : relation names in this bag
    - proj_global, index_global : precomputed once
    - constraints : partial assignment from parent bags; any variable
                    in constraints that appears in the bag is fixed.

    Returns:
        list of dicts mapping bag.vars -> int values.
    """

    vars_order = bag.vars
    V = set(vars_order)

    # Edges in this bag: (rel_name, [attrs_in_bag])
    edges: List[Tuple[str, List[str]]] = []
    for rel in bag.lambdas:
        attrs = SCHEMAS[rel]
        bag_attrs = [a for a in attrs if a in V]
        if bag_attrs:
            edges.append((rel, bag_attrs))

    results: List[Dict[str, int]] = []

    def get_allowed(var: str, prefix: Dict[str, int]) -> List[int]:
        """Compute allowed values for 'var' given 'prefix' and constraints."""
        candidate_sets: List[Set[int]] = []

        for rel, attrs in edges:
            if var not in attrs:
                continue

            # Unary case (won't actually occur here, but keep it general)
            if len(attrs) == 1:
                candidate_sets.append(proj_global[(rel, var)])
                continue

            a, b = attrs
            other = b if var == a else a

            # Determine if other variable has a known value (from prefix or constraints)
            other_val = None
            if other in prefix:
                other_val = prefix[other]
            elif constraints and other in constraints:
                other_val = constraints[other]

            if other_val is None:
                # No restriction: full projection of var in this relation
                candidate_sets.append(proj_global[(rel, var)])
            else:
                # Restricted by adjacency from global index
                neigh_map = index_global[(rel, other)]
                candidate_sets.append(neigh_map.get(other_val, set()))

        if not candidate_sets:
            # Var not involved in any relation in this bag
            return []

        # Intersect candidate sets (WCOJ-style)
        candidate_sets.sort(key=len)
        vals = set(candidate_sets[0])
        for s in candidate_sets[1:]:
            vals &= s
            if not vals:
                break

        # Apply constraint if var is fixed by parent
        if constraints and var in constraints:
            fixed = constraints[var]
            if fixed in vals:
                vals = {fixed}
            else:
                vals = set()  # no possible value

        return sorted(vals)

    def recurse(i: int, prefix: Dict[str, int]):
        if i == len(vars_order):
            results.append(prefix.copy())
            return

        var = vars_order[i]

        # If already fixed in prefix (shouldn't happen), move on
        if var in prefix:
            recurse(i + 1, prefix)
            return

        allowed_vals = get_allowed(var, prefix)
        if not allowed_vals:
            return

        for v in allowed_vals:
            prefix[var] = v
            recurse(i + 1, prefix)
            del prefix[var]

    recurse(0, {})
    return results


# ===============================================================
#  ENUMERATION OVER THE FHW TREE (LAZY BAG EVALUATION)
# ===============================================================
def enumerate_fhw(
    bags: Dict[str, FBag],
    proj_global: Dict[Tuple[str, str], Set[int]],
    index_global: Dict[Tuple[str, str], Dict[int, Set[int]]],
    root: str = "B1",
) -> List[Tuple[int, ...]]:
    """
    Enumerate full results of the query using:
      - root bag evaluated once,
      - all other bags evaluated lazily given parent assignments,
      - global indexes (no rescanning of relations).
    """

    results: List[Tuple[int, ...]] = []

    root_bag = bags[root]
    root_rows = bag_generic_join(root_bag, proj_global, index_global, constraints=None)

    def dfs(bname: str, assignment: Dict[str, int]):
        bag = bags[bname]

        # Get rows for this bag under current assignment
        if bname == root:
            rows = root_rows
        else:
            rows = bag_generic_join(bag, proj_global, index_global, constraints=assignment)

        shared = [v for v in bag.vars if v in assignment]

        for row in rows:
            # Check consistency with existing assignment
            if any(row[v] != assignment[v] for v in shared):
                continue

            extended = assignment.copy()
            for v in bag.vars:
                extended.setdefault(v, row[v])

            if not bag.children:
                # Only B4 "completes" the query in this decomposition
                if bag.name == "B4":
                    # Emit full tuple only if all attributes are present
                    if all(a in extended for a in ATTR_ORDER):
                        results.append(tuple(extended[a] for a in ATTR_ORDER))
            else:
                for child_name in bag.children:
                    dfs(child_name, extended)

    dfs(root, {})
    # De-duplicate just in case
    results = list(dict.fromkeys(results))
    return results


# ===============================================================
#  MAIN: FHW EVALUATION (LAZY + GLOBAL INDEXES)
# ===============================================================
def fhw_lazy_evaluate(relations_dir: str = "query_relations") -> List[Tuple[int, ...]]:
    print("Loading relations...")
    relations = load_relations(relations_dir)

    print("Building fractional hypertree decomposition...")
    bags = build_fractional_bags()

    print("Building global indexes (projections + adjacency)...")
    proj_global, index_global = build_global_indexes(relations)

    print("Running FHW evaluation (bag-local WCOJ with global indexes)...")
    start = time.time()
    output = enumerate_fhw(bags, proj_global, index_global, root="B1")
    end = time.time()

    print(f"Number of result tuples: {len(output)}")
    print(f"Runtime: {end - start:.4f} seconds")
    return output


if __name__ == "__main__":
    fhw_lazy_evaluate("query_relations")
