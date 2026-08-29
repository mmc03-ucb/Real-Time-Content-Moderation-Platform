"""
These test the batched path, because that is the one the pipeline uses:
one Redis round trip answers every question about a batch of messages.
"""

from moderation.defenses import dedup, raid, risk, signals
from moderation.ingest.generator import make_message


def ask(fake_redis, messages, limit=10, window=10.0, raid_threshold=25, now=1000.0):
    return signals.gather(fake_redis, messages, limit, window, raid_threshold, now=now)


def chat(text="hello there friend", user="u1", stream="s1", age=400):
    return make_message(user, text, stream_id=stream, client_meta={"account_age_days": age})


def signal_for(batch, message):
    return batch[message["msg_id"]]


# ----------------------------------------------------------------- dedup

def test_the_same_message_twice_is_a_duplicate(fake_redis):
    first, second = chat("buy my stuff now"), chat("buy my stuff now")
    assert not signal_for(ask(fake_redis, [first]), first).duplicate
    assert signal_for(ask(fake_redis, [second]), second).duplicate


def test_a_repeat_inside_one_batch_is_caught(fake_redis):
    first, second = chat("buy my stuff now"), chat("buy my stuff now")
    batch = ask(fake_redis, [first, second])
    assert not signal_for(batch, first).duplicate
    assert signal_for(batch, second).duplicate


def test_duplicate_check_ignores_case_and_spacing(fake_redis):
    first, second = chat("Buy My Stuff Now"), chat("  buy my stuff now  ")
    ask(fake_redis, [first])
    assert signal_for(ask(fake_redis, [second]), second).duplicate


def test_two_users_saying_the_same_thing_are_not_duplicates(fake_redis):
    first, second = chat("buy my stuff now", user="u1"), chat("buy my stuff now", user="u2")
    ask(fake_redis, [first])
    assert not signal_for(ask(fake_redis, [second]), second).duplicate


def test_short_reactions_are_allowed_to_repeat(fake_redis):
    first, second = chat("gg"), chat("gg")
    ask(fake_redis, [first])
    assert not signal_for(ask(fake_redis, [second]), second).duplicate


def test_the_dedup_key_depends_on_the_user_and_the_text():
    assert dedup.key("u1", "hello") != dedup.key("u2", "hello")
    assert dedup.key("u1", "Hello ") == dedup.key("u1", "hello")


# ------------------------------------------------------------ rate limit

def test_users_under_the_limit_are_fine(fake_redis):
    for i in range(5):
        message = chat(f"message number {i}")
        assert not signal_for(ask(fake_redis, [message], limit=5), message).over_rate_limit


def test_going_over_the_limit_is_flagged(fake_redis):
    for i in range(5):
        ask(fake_redis, [chat(f"message number {i}")], limit=5)
    late = chat("one message too many")
    assert signal_for(ask(fake_redis, [late], limit=5), late).over_rate_limit


def test_the_window_slides(fake_redis):
    for i in range(5):
        ask(fake_redis, [chat(f"message number {i}")], limit=5, now=1000 + i)
    # Eleven seconds later the earlier messages no longer count.
    later = chat("still chatting away")
    assert not signal_for(ask(fake_redis, [later], limit=5, now=1015), later).over_rate_limit


def test_limits_are_per_user(fake_redis):
    for i in range(6):
        ask(fake_redis, [chat(f"noisy message {i}", user="noisy")], limit=5)
    quiet = chat("hello everyone", user="quiet")
    assert not signal_for(ask(fake_redis, [quiet], limit=5), quiet).over_rate_limit


# ------------------------------------------------------------ risk score

def test_unknown_users_start_clean(fake_redis):
    message = chat()
    assert signal_for(ask(fake_redis, [message]), message).risk_score == 0.0


def test_violations_stack_up(fake_redis):
    signals.commit(fake_redis, set(), [("u1", 0.0, 1.0)], now=1000)
    signals.commit(fake_redis, set(), [("u1", 1.0, 1.0)], now=1000)
    message = chat()
    assert signal_for(ask(fake_redis, [message], now=1000), message).risk_score == 2.0


def test_risk_fades_over_time():
    assert risk.decayed(4.0, updated_at=0, now=risk.HALF_LIFE_SECONDS) == 2.0


# -------------------------------------------------------------- raid mode

def new_account(user, stream="s1"):
    return chat("hello everyone", user=user, stream=stream, age=0)


def test_a_crowd_of_new_accounts_triggers_raid_mode(fake_redis):
    raiders = [new_account(f"u{i}") for i in range(10)]
    batch = ask(fake_redis, raiders, raid_threshold=10)
    assert batch.new_raids == {"s1"}


def test_raid_mode_lasts_into_the_next_batch(fake_redis):
    batch = ask(fake_redis, [new_account(f"u{i}") for i in range(10)], raid_threshold=10)
    signals.commit(fake_redis, batch.new_raids, [])
    latecomer = new_account("u99")
    assert signal_for(ask(fake_redis, [latecomer], raid_threshold=10), latecomer).raid_mode


def test_one_account_repeating_itself_is_not_a_raid(fake_redis):
    same = [new_account("same_user") for _ in range(50)]
    assert ask(fake_redis, same, raid_threshold=10).new_raids == set()


def test_established_accounts_do_not_count_towards_a_raid(fake_redis):
    regulars = [chat("hello everyone", user=f"u{i}", age=500) for i in range(50)]
    assert ask(fake_redis, regulars, raid_threshold=10).new_raids == set()


def test_raid_mode_is_per_stream(fake_redis):
    batch = ask(fake_redis, [new_account(f"u{i}", "s1") for i in range(10)],
                raid_threshold=10)
    signals.commit(fake_redis, batch.new_raids, [])
    elsewhere = new_account("u99", "s2")
    assert not signal_for(ask(fake_redis, [elsewhere], raid_threshold=10),
                          elsewhere).raid_mode


def test_only_young_accounts_count_as_new():
    assert raid.is_new_account(chat(age=0))
    assert not raid.is_new_account(chat(age=100))
    assert not raid.is_new_account(make_message("u", "hi"))
