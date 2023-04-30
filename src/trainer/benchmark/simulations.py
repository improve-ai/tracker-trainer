# from coba.environments.simulated.synthetics import LambdaSimulation
from coba import LambdaSimulation

class HappySunday(LambdaSimulation):

    def __init__(self,
        n_interactions: int = 3000, rng=None) -> None:

        # HAVE_A_GREAT_DAY = "Have a great day!"
        # HAVE_AN_OK_DAY = "Have an OK day."
        # HAPPY_SUNDAY = "Happy Sunday!"

        HAVE_A_GREAT_DAY =[1, 0, 0]
        HAVE_AN_OK_DAY = [0, 1, 0]
        HAPPY_SUNDAY = [0, 0, 1]

        rng = rng

        def context(index: int):
            return [rng.randint(0,6)]

        def actions(index: int, context):
            return [HAVE_A_GREAT_DAY, HAVE_AN_OK_DAY, HAPPY_SUNDAY]

        def reward(index: int, context, action):
            if action == HAVE_A_GREAT_DAY:
                return 10 / 1000
            elif action == HAVE_AN_OK_DAY:
                return 1 / 1000
            elif context == [0]: # 0 is Sunday
                return 1000 / 1000
            else:
                return 0

        super().__init__(n_interactions, context, actions, reward)


class HappySundayStrings(LambdaSimulation):

    def __init__(self,n_interactions: int = 3000, rng=None) -> None:

        HAVE_A_GREAT_DAY = "Have a great day!"
        HAVE_AN_OK_DAY = "Have an OK day."
        HAPPY_SUNDAY = "Happy Sunday!"

        rng = rng

        def context(index: int):
            return rng.randint(0, 6)

        def actions(index: int, context):
            return [HAVE_A_GREAT_DAY, HAVE_AN_OK_DAY, HAPPY_SUNDAY]

        def reward(index: int, context, action):
            if action == HAVE_A_GREAT_DAY:
                return 10 / 1000
            elif action == HAVE_AN_OK_DAY:
                return 1 / 1000
            elif context == 0:  # 0 is Sunday
                return 1000 / 1000
            else:
                return 0

        super().__init__(n_interactions, context, actions, reward)


class LinearValueMatcher(LambdaSimulation):

    def __init__(self, n_interactions: int = 3000, matches: int = 10, rng=None) -> None:

        # rng = CobaRandom()
        rng = rng

        def context(interaction):
            return [rng.randint(0, matches-1)]

        def actions(interaction, context):
            return list(range(matches))

        def reward(interaction, context, action):
            return 1 if context == [action] else 0

        super().__init__(n_interactions, context, actions, reward)


class NonLinearValueMatcher(LambdaSimulation):

    def __init__(self, n_interactions: int = 3000, matches: int = 10, rng=None) -> None:

        rng = rng

        _actions = list(range(matches))
        shuffled = rng.shuffle(_actions)

        def context(interaction):
            return [rng.randint(0, matches-1)]

        def actions(interaction, context):
            return _actions

        def reward(interaction, context, action):
            return 1 if context == [shuffled[action]] else 0

        super().__init__(n_interactions, context, actions, reward)


class FeatureMatcher(LambdaSimulation):

    def __init__(self, n_interactions: int = 3000, matches: int = 10, rng=None) -> None:

        rng = rng

        def context(interaction):
            _context = [0] * matches
            _context[rng.randint(0, matches-1)] = 1
            return _context

        def actions(interaction, context):
            _actions = [ [0] * matches for i in range(matches)]
            for i in range(matches):
                _actions[i][i] = 1

            return _actions

        def reward(interaction, context, action):
            return 1 if context == action else 0

        super().__init__(n_interactions, context, actions, reward)


class CompareValuePairs(LambdaSimulation):

    def __init__(self, n_interactions: int = 3000, n_variants: int = 100, rng=None) -> None:

        rng = rng
        variants = list(range(n_variants))

        def context(interaction):
            return None

        def actions(interaction, context):
            while True:
                a = rng.choice(variants)
                b = rng.choice(variants)
                if a != b:
                    return [a, b]

        def reward(interaction, context, action):
            return action / n_variants

        super().__init__(n_interactions, context, actions, reward)


class CompareFeaturePairs(LambdaSimulation):

    def __init__(self, n_interactions: int = 3000, n_variants: int = 100, rng=None) -> None:

        rng = rng

        variants = list(range(n_variants))

        def context(interaction):
            return None

        def actions(interaction, context):
            while True:
                a = [0] * n_variants
                a[rng.choice(range(n_variants))] = 1
                b = [0] * n_variants
                b[rng.choice(range(n_variants))] = 1
                if a != b:
                    return [a, b]

        def reward(interaction, context, action):
            return action.index(1) / n_variants

        super().__init__(n_interactions, context, actions, reward)
