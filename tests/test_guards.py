from bcia.guards import check_claims, quoted_claims

APPROVED = {"helps skin feel hydrated", "supports a smoother-looking appearance"}


def test_approved_v2_phrase_passes():                       # acceptance A1
    assert check_claims('You can say "helps skin feel hydrated" (DOC001 v2).', APPROVED) == []


def test_superseded_v1_phrase_is_blocked():                 # acceptance A1, failure case
    v = check_claims('Use "supports a hydrated appearance".', APPROVED)
    assert [x.rule for x in v] == ["not_in_register"]


def test_quoted_medical_claim_is_blocked():                 # acceptance A8
    v = check_claims('Suggested copy: "cures dryness overnight".', APPROVED)
    assert [x.rule for x in v] == ["banned_verb"]


def test_explaining_the_ban_outside_quotes_is_allowed():
    assert check_claims("You cannot say the serum cures dryness.", APPROVED) == []


def test_curly_quotes_are_checked_too():
    assert quoted_claims("Try “leaves skin glowing”") == ["leaves skin glowing"]
    assert check_claims("Try “leaves skin glowing”", APPROVED)[0].rule == "not_in_register"
