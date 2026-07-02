import json

from pgmon.analysis.plandiff import analyze_plan, compare_plans

PLAN_INDEX = {
    "Plan": {
        "Node Type": "Index Scan",
        "Relation Name": "orders",
        "Index Name": "orders_customer_idx",
        "Startup Cost": 0.42,
        "Total Cost": 8.44,
        "Plan Rows": 1,
        "Index Cond": "(customer_id = 42)",
    }
}

PLAN_SEQSCAN = {
    "Plan": {
        "Node Type": "Seq Scan",
        "Relation Name": "orders",
        "Startup Cost": 0.0,
        "Total Cost": 15000.0,
        "Plan Rows": 100,
        "Filter": "(customer_id = 42)",
    }
}

PLAN_OTHER_INDEX = {
    "Plan": {
        "Node Type": "Index Scan",
        "Relation Name": "orders",
        "Index Name": "orders_created_idx",
        "Startup Cost": 0.42,
        "Total Cost": 120.0,
        "Plan Rows": 10,
    }
}

PLAN_JOIN = {
    "Plan": {
        "Node Type": "Hash Join",
        "Join Type": "Inner",
        "Startup Cost": 1.0,
        "Total Cost": 100.0,
        "Plan Rows": 5,
        "Plans": [
            {"Node Type": "Seq Scan", "Relation Name": "a",
             "Startup Cost": 0, "Total Cost": 10, "Plan Rows": 100},
            {"Node Type": "Hash", "Startup Cost": 0, "Total Cost": 20, "Plan Rows": 100,
             "Plans": [{"Node Type": "Seq Scan", "Relation Name": "b",
                        "Startup Cost": 0, "Total Cost": 10, "Plan Rows": 100}]},
        ],
    }
}


def test_fingerprint_stable_under_cost_changes():
    a = analyze_plan(PLAN_INDEX)
    changed = json.loads(json.dumps(PLAN_INDEX))
    changed["Plan"]["Total Cost"] = 999.0
    changed["Plan"]["Plan Rows"] = 12345
    b = analyze_plan(changed)
    assert a.fingerprint == b.fingerprint


def test_fingerprint_differs_on_structure():
    assert analyze_plan(PLAN_INDEX).fingerprint != analyze_plan(PLAN_SEQSCAN).fingerprint


def test_analyze_extracts_metadata():
    info = analyze_plan(PLAN_INDEX)
    assert info.tables == ["orders"]
    assert info.indexes == ["orders_customer_idx"]
    assert info.seq_scan_tables == []

    info2 = analyze_plan(PLAN_SEQSCAN)
    assert info2.seq_scan_tables == ["orders"]
    assert info2.seq_scan_filters[0]["filter"] == "(customer_id = 42)"


def test_analyze_accepts_explain_list_format():
    info = analyze_plan([PLAN_INDEX])
    assert info.tables == ["orders"]


def test_index_to_seqscan_is_critical():
    changes = compare_plans(analyze_plan(PLAN_INDEX), analyze_plan(PLAN_SEQSCAN))
    assert any(c.kind == "index_to_seqscan" and c.severity == "critical" for c in changes)


def test_seqscan_to_index_is_info():
    changes = compare_plans(analyze_plan(PLAN_SEQSCAN), analyze_plan(PLAN_INDEX))
    assert any(c.kind == "seqscan_to_index" and c.severity == "info" for c in changes)


def test_index_change_is_warning():
    changes = compare_plans(analyze_plan(PLAN_INDEX), analyze_plan(PLAN_OTHER_INDEX))
    assert any(c.kind == "index_changed" and c.severity == "warning" for c in changes)


def test_identical_plans_produce_no_changes():
    assert compare_plans(analyze_plan(PLAN_INDEX), analyze_plan(PLAN_INDEX)) == []


def test_cost_increase_flagged():
    cheap = analyze_plan(PLAN_JOIN)
    expensive_json = json.loads(json.dumps(PLAN_JOIN))
    expensive_json["Plan"]["Total Cost"] = 10000.0
    expensive_json["Plan"]["Node Type"] = "Merge Join"  # меняем структуру
    changes = compare_plans(cheap, analyze_plan(expensive_json), cost_ratio_warning=5.0)
    kinds = {c.kind for c in changes}
    assert "cost_increase" in kinds
    assert "join_changed" in kinds
