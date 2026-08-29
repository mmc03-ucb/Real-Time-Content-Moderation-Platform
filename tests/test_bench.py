from moderation.loadtest.bench import percentile, report


def test_percentiles_of_a_simple_range():
    values = list(range(1, 101))
    assert percentile(values, 0.5) == 50
    assert percentile(values, 0.99) == 99


def test_percentile_of_nothing_is_zero():
    assert percentile([], 0.99) == 0.0


def test_the_report_flags_messages_that_never_came_back():
    result = report(sent=100, produce_seconds=1.0, latencies=[5.0] * 90,
                    actions={"allow": 90}, settle_seconds=1.0)
    assert result["missing"] == 10
    assert result["decided"] == 90
    assert result["timed_out"] is False


def test_a_clean_run_accounts_for_everything():
    result = report(sent=10, produce_seconds=1.0, latencies=[1.0] * 10,
                    actions={"allow": 10}, settle_seconds=2.0)
    assert result["missing"] == 0
    assert result["produced_per_second"] == 10.0
    assert result["decided_per_second"] == 5.0
