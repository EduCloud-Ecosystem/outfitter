"""Fail closed when deliberate decision faults defeat the candidate model oracle."""
import json
import finite_model


def main():
    original = finite_model.admit
    results = []
    try:
        for label, decision in (("always_accept", True), ("always_reject", False)):
            finite_model.admit = lambda *_args, answer=decision: answer
            try:
                finite_model.exercise()
            except AssertionError as exc:
                results.append({"fault": label, "oracle_rejected": True, "reason": str(exc)})
            else:
                raise RuntimeError(f"candidate model oracle silently accepted {label}")
    finally:
        finite_model.admit = original
    print(json.dumps({"python_assertions_enabled": __debug__, "faults": results}))


if __name__ == "__main__":
    main()
