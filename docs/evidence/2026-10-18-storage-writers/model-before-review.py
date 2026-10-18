"""Disposable candidate rule; never imported by broker/runtime. Not enforcement."""
from copy import deepcopy
from hashlib import sha256


def admit(plan, observation):
    """No release/reuse from identity, length, process exit, or absence alone."""
    if observation["owner"] != plan["owner"]:
        return False
    if not observation["terminal"] or not observation["handles_closed"]:
        return False
    if observation["files"] != plan["files"]:
        return False
    if not observation["all_owners_known"] or not observation["scratch_reusable"]:
        return False
    for dimension in ("bytes", "entries"):
        terms = observation[dimension]
        if set(terms) != {"observed", "owned_remaining", "new", "safety", "limit"}:
            return False
        if any(type(v) is not int or v < 0 for v in terms.values()):
            return False
        if sum(terms[k] for k in ("observed", "owned_remaining", "new", "safety")) > terms["limit"]:
            return False
    return True


def exercise():
    # Expected identity and content exist before any candidate observation.
    plan = {"owner": ["authority", "request", "output", "boot", "incarnation-1"],
            "files": {"outputs/planned.json": {"bytes": 3, "sha256": sha256(b"abc").hexdigest()}}}
    base = {"owner": deepcopy(plan["owner"]), "files": deepcopy(plan["files"]),
            "terminal": True, "handles_closed": True, "all_owners_known": True,
            "scratch_reusable": True,
            "bytes": {"observed": 10, "owned_remaining": 5, "new": 3, "safety": 2, "limit": 20},
            "entries": {"observed": 4, "owned_remaining": 1, "new": 1, "safety": 1, "limit": 7}}
    results = []
    assert admit(plan, base), "exact-boundary candidate should pass model"
    results.append({"id": "exact-boundary", "accepted_by_model": True})
    faults = {
        "wrong-authority": lambda x: x["owner"].__setitem__(0, "foreign"),
        "wrong-incarnation": lambda x: x["owner"].__setitem__(4, "incarnation-2"),
        "nonterminal-writer": lambda x: x.__setitem__("terminal", False),
        "open-handles": lambda x: x.__setitem__("handles_closed", False),
        "same-length-corruption": lambda x: x["files"]["outputs/planned.json"].__setitem__("sha256", sha256(b"abd").hexdigest()),
        "unowned-growth": lambda x: x["files"].__setitem__("unknown", {"bytes": 1, "sha256": sha256(b"x").hexdigest()}),
        "missing-path-is-not-release": lambda x: x["files"].clear(),
        "unknown-writer": lambda x: x.__setitem__("all_owners_known", False),
        "unproven-scratch-reuse": lambda x: x.__setitem__("scratch_reusable", False),
        "unknown-scratch-bytes": lambda x: x["bytes"].__setitem__("safety", "UNKNOWN"),
        "unknown-entries": lambda x: x["entries"].__setitem__("owned_remaining", None),
        "byte-peak-exceeds-limit": lambda x: x["bytes"].__setitem__("new", 4),
        "entry-peak-exceeds-limit": lambda x: x["entries"].__setitem__("new", 2),
    }
    for name, fault in faults.items():
        candidate = deepcopy(base)
        fault(candidate)
        accepted = admit(plan, candidate)
        assert not accepted, f"counterexample admitted: {name}"
        results.append({"id": name, "accepted_by_model": accepted})
    return {"status": "MODEL_ONLY_NOT_ENFORCED", "cases": results,
            "production_observation_connection": "none; candidate ownership rule only"}
