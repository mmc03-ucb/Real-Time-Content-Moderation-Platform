from moderation.ml.classifier import LexiconClassifier, SafeClassifier, load_classifier


def test_clean_chat_scores_low():
    assert LexiconClassifier().score_batch(["gg wp", "hello everyone"]) == [0.0, 0.0]


def test_insults_score_high():
    score = LexiconClassifier().score_batch(["you are an idiot"])[0]
    assert score > 0.5


def test_scores_stay_between_zero_and_one():
    nasty = "you stupid worthless idiot moron loser clown trash"
    assert LexiconClassifier().score_batch([nasty]) == [1.0]


def test_one_call_scores_the_whole_batch():
    calls = []

    class Counting:
        name = "counting"

        def score_batch(self, texts):
            calls.append(len(texts))
            return [0.0] * len(texts)

    SafeClassifier(Counting()).score_batch(["a", "b", "c"])
    assert calls == [3]


def test_a_broken_model_returns_no_scores_instead_of_crashing():
    class Broken:
        name = "broken"

        def score_batch(self, texts):
            raise RuntimeError("model is down")

    classifier = SafeClassifier(Broken())
    assert classifier.score_batch(["hi", "there"]) == [None, None]
    assert classifier.healthy is False


def test_the_model_is_marked_healthy_again_once_it_recovers():
    class Flaky:
        name = "flaky"

        def __init__(self):
            self.fail = True

        def score_batch(self, texts):
            if self.fail:
                raise RuntimeError("down")
            return [0.1] * len(texts)

    inner = Flaky()
    classifier = SafeClassifier(inner)
    classifier.score_batch(["hi"])
    inner.fail = False
    assert classifier.score_batch(["hi"]) == [0.1]
    assert classifier.healthy is True


def test_a_classifier_is_always_available():
    assert load_classifier(prefer_detoxify=False).score_batch(["hi"]) == [0.0]
