def effect_ratio(df, advanced, baseline):

    topics_on_source = set([i for i in df[df['collection'] == 'source'].topic.unique()])
    topics_on_target = set([i for i in df[df['collection'] != 'source'].topic.unique()])
    topics = topics_on_source.intersection(topics_on_target)

    sum_numerator = 0
    sum_denominator = 0

    for topic in topics:
        sum_numerator += improvements_in_experiments(
            df=df,
            advanced=advanced,
            baseline=baseline,
            topic=topic,
            on_old_collection=False
        )
        sum_denominator += improvements_in_experiments(
            df=df,
            advanced=advanced,
            baseline=baseline,
            topic=topic,
            on_old_collection=True
        )

    return (sum_numerator/len(topics))/(sum_denominator/len(topics))


def improvements_in_experiments(df, advanced, baseline, topic, on_old_collection):
    if on_old_collection:
        df = df[df['collection'] == 'source']
    else:
        df = df[df['collection'] != 'source']

    advanced_score = unique_score_or_fail(df, advanced, topic)
    baseline_score = unique_score_or_fail(df, baseline, topic)

    return advanced_score - baseline_score


def unique_score_or_fail(df, method, topic):
    df = df[(df['topic'].astype(int) == int(topic)) & (df['tag'] == method)]
    if len(df) != 1:
        raise ValueError('Couldnt handle df of length ' + str(len(df)) + ' using '
                         + str(method) + ' under ' + str(topic))

    return df.iloc[0]['measure']


def delta_relative_improvement(df, advanced, baseline):
    return relative_improvement(df, advanced, baseline, True) - relative_improvement(df, advanced, baseline, False)


def classify_repro_pairs(df):
    from collections import Counter
    ret = Counter()

    for _, r in df.iterrows():
        c = Counter(classify_repro_pair(r))
        ret = ret + c

    return dict(ret)


def classify_repro_pair(repro_pair):
    er = repro_pair['effectRatio']
    delta_ri = repro_pair['delta_relative_improvement']

    if er > 0 and delta_ri > 0:
        return {
            'effect-size-success-absolute-scores-failure': 1,
            'effect-size-success-absolute-scores-success': 0,
            'effect-size-failure-absolute-scores-failure': 0,
            'effect-size-failure-absolute-scores-success': 0,
        }
    elif er < 0 and delta_ri < 0:
        return {
            'effect-size-success-absolute-scores-failure': 0,
            'effect-size-success-absolute-scores-success': 0,
            'effect-size-failure-absolute-scores-failure': 0,
            'effect-size-failure-absolute-scores-success': 1,
        }
    elif er > 0 and delta_ri < 0:
        return {
            'effect-size-success-absolute-scores-failure': 0,
            'effect-size-success-absolute-scores-success': 1,
            'effect-size-failure-absolute-scores-failure': 0,
            'effect-size-failure-absolute-scores-success': 0,
        }

    return {
        'effect-size-success-absolute-scores-failure': 0,
        'effect-size-success-absolute-scores-success': 0,
        'effect-size-failure-absolute-scores-failure': 1,
        'effect-size-failure-absolute-scores-success': 0,
    }


def relative_improvement(df, advanced, baseline, on_old_collection):
    a_avg = avg_score(df, advanced, on_old_collection)
    b_avg = avg_score(df, baseline, on_old_collection)

    return (a_avg-b_avg)/b_avg


def avg_score(df, method, on_old_collection):
    if on_old_collection:
        df = df[df['collection'] == 'source']
    else:
        df = df[df['collection'] != 'source']

    df = df[df['tag'] == method]

    return float(df.measure.mean())
