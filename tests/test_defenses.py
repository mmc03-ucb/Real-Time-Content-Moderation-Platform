from moderation.defenses import dedup, raid, rate_limit, risk
from moderation.ingest.generator import make_message


# ------------------------------------------------------------ rate limit

def test_users_under_the_limit_are_allowed(fake_redis):
    assert all(rate_limit.allow(fake_redis, "user:u1", limit=5) for _ in range(5))


def test_going_over_the_limit_is_blocked(fake_redis):
    for _ in range(5):
        rate_limit.allow(fake_redis, "user:u1", limit=5)
    assert not rate_limit.allow(fake_redis, "user:u1", limit=5)


def test_the_window_slides(fake_redis):
    for i in range(5):
        rate_limit.allow(fake_redis, "user:u1", limit=5, window_seconds=10, now=1000 + i)
    # Eleven seconds later the earlier messages no longer count.
    assert rate_limit.allow(fake_redis, "user:u1", limit=5, window_seconds=10, now=1015)


def test_limits_are_per_user(fake_redis):
    for _ in range(6):
        rate_limit.allow(fake_redis, "user:noisy", limit=5)
    assert rate_limit.allow(fake_redis, "user:quiet", limit=5)


# ----------------------------------------------------------------- dedup

def test_the_same_message_twice_is_a_duplicate(fake_redis):
    assert not dedup.is_duplicate(fake_redis, "u1", "buy my stuff now")
    assert dedup.is_duplicate(fake_redis, "u1", "buy my stuff now")


def test_duplicate_check_ignores_case_and_spacing(fake_redis):
    dedup.is_duplicate(fake_redis, "u1", "Buy My Stuff Now")
    assert dedup.is_duplicate(fake_redis, "u1", "  buy my stuff now ")


def test_two_users_saying_the_same_thing_are_not_duplicates(fake_redis):
    dedup.is_duplicate(fake_redis, "u1", "buy my stuff now")
    assert not dedup.is_duplicate(fake_redis, "u2", "buy my stuff now")


def test_short_reactions_are_allowed_to_repeat(fake_redis):
    dedup.is_duplicate(fake_redis, "u1", "gg")
    assert not dedup.is_duplicate(fake_redis, "u1", "gg")


# ------------------------------------------------------------ risk score

def test_unknown_users_start_clean(fake_redis):
    assert risk.get_score(fake_redis, "u1") == 0.0


def test_violations_stack_up(fake_redis):
    risk.add_violation(fake_redis, "u1", 1.0, now=0)
    assert risk.add_violation(fake_redis, "u1", 1.0, now=0) == 2.0


def test_risk_fades_over_time(fake_redis):
    risk.add_violation(fake_redis, "u1", 4.0, now=0)
    assert risk.get_score(fake_redis, "u1", now=risk.HALF_LIFE_SECONDS) == 2.0


# -------------------------------------------------------------- raid mode

def new_account(stream, user):
    return make_message(user, "hi", stream_id=stream, client_meta={"account_age_days": 0})


def test_a_crowd_of_new_accounts_triggers_raid_mode(fake_redis):
    for i in range(9):
        assert not raid.observe(fake_redis, new_account("s1", f"u{i}"), threshold=10, now=0)
    assert raid.observe(fake_redis, new_account("s1", "u9"), threshold=10, now=0)
    assert raid.in_raid_mode(fake_redis, "s1")


def test_one_account_repeating_itself_is_not_a_raid(fake_redis):
    for _ in range(50):
        raid.observe(fake_redis, new_account("s1", "same_user"), threshold=10, now=0)
    assert not raid.in_raid_mode(fake_redis, "s1")


def test_established_accounts_do_not_count_towards_a_raid(fake_redis):
    for i in range(50):
        msg = make_message(f"u{i}", "hi", stream_id="s1",
                           client_meta={"account_age_days": 500})
        raid.observe(fake_redis, msg, threshold=10, now=0)
    assert not raid.in_raid_mode(fake_redis, "s1")


def test_raid_mode_is_per_stream(fake_redis):
    for i in range(10):
        raid.observe(fake_redis, new_account("s1", f"u{i}"), threshold=10, now=0)
    assert not raid.in_raid_mode(fake_redis, "s2")
