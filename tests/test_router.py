from bcia.router import route


def test_metric_question_is_structured():
    assert route("What was CAM009's ROAS?").route == "structured"


def test_claims_question_is_unstructured():
    assert route("What hydration wording is approved for the Aurora serum?").route == "unstructured"


def test_mixed_question_is_both():
    assert route("How did CAM009 perform and which claims are approved?").route == "both"


def test_promotion_without_market_asks_to_clarify():                   # DOC011 vs DOC015 trap
    assert route("What is the retail promotion rule?").route == "clarify"


def test_promotion_with_market_does_not_clarify():
    assert route("What is the retail promotion policy in Boston?").route != "clarify"
