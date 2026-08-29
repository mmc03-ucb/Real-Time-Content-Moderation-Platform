from moderation.ingest.loadgen import ChatSimulator, run


def test_messages_stay_inside_the_configured_streams():
    sim = ChatSimulator(streams=3, seed=1)
    streams = {sim.next_message()["stream_id"] for _ in range(200)}
    assert streams <= {"stream_0", "stream_1", "stream_2"}


def test_toxic_ratio_is_roughly_respected():
    sim = ChatSimulator(streams=1, toxic_ratio=0.5, spam_ratio=0.0,
                        link_ratio=0.0, seed=7)
    from moderation.ingest import corpus
    toxic = sum(1 for _ in range(1000) if sim.next_message()["text"] in corpus.TOXIC)
    assert 400 < toxic < 600


def test_raid_floods_one_stream_with_new_accounts():
    sim = ChatSimulator(streams=5, seed=3)
    sim.start_raid("stream_2")
    msgs = [sim.next_message() for _ in range(200)]
    raiders = [m for m in msgs if m["user_id"].startswith("raider_")]
    assert len(raiders) > 100
    assert all(m["stream_id"] == "stream_2" for m in raiders)
    assert all(m["client_meta"]["account_age_days"] == 0 for m in raiders)


def test_run_sends_about_the_requested_number_of_messages():
    sim = ChatSimulator(streams=2, seed=5)
    sent = []
    total = run(sim, sent.append, rate=200, duration=0.25)
    assert 20 <= total <= 80
    assert len(sent) == total
