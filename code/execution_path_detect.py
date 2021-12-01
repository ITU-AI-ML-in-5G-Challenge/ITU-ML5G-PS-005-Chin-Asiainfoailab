import keras
import numpy as np
from log_key_lstm_model import log_key_model
import os
import pickle

def raise_unorderable_types(ordering, a, b):
    raise TypeError(
        "unorderable types: %s() %s %s()"
        % (type(a).__name__, ordering, type(b).__name__)
    )

    
from collections import defaultdict, Counter
from functools import reduce    
    
class FreqDist(Counter):
    """
    A frequency distribution for the outcomes of an experiment.  A
    frequency distribution records the number of times each outcome of
    an experiment has occurred.  For example, a frequency distribution
    could be used to record the frequency of each word type in a
    document.  Formally, a frequency distribution can be defined as a
    function mapping from each sample to the number of times that
    sample occurred as an outcome.

    Frequency distributions are generally constructed by running a
    number of experiments, and incrementing the count for a sample
    every time it is an outcome of an experiment.  For example, the
    following code will produce a frequency distribution that encodes
    how often each word occurs in a text:

        >>> from nltk.tokenize import word_tokenize
        >>> from nltk.probability import FreqDist
        >>> sent = 'This is an example sentence'
        >>> fdist = FreqDist()
        >>> for word in word_tokenize(sent):
        ...    fdist[word.lower()] += 1

    An equivalent way to do this is with the initializer:

        >>> fdist = FreqDist(word.lower() for word in word_tokenize(sent))

    """

    def __init__(self, samples=None):
        """
        Construct a new frequency distribution.  If ``samples`` is
        given, then the frequency distribution will be initialized
        with the count of each object in ``samples``; otherwise, it
        will be initialized to be empty.

        In particular, ``FreqDist()`` returns an empty frequency
        distribution; and ``FreqDist(samples)`` first creates an empty
        frequency distribution, and then calls ``update`` with the
        list ``samples``.

        :param samples: The samples to initialize the frequency
            distribution with.
        :type samples: Sequence
        """
        Counter.__init__(self, samples)

        # Cached number of samples in this FreqDist
        self._N = None

    def N(self):
        """
        Return the total number of sample outcomes that have been
        recorded by this FreqDist.  For the number of unique
        sample values (or bins) with counts greater than zero, use
        ``FreqDist.B()``.

        :rtype: int
        """
        if self._N is None:
            # Not already cached, or cache has been invalidated
            self._N = sum(self.values())
        return self._N

    def __setitem__(self, key, val):
        """
        Override ``Counter.__setitem__()`` to invalidate the cached N
        """
        self._N = None
        super(FreqDist, self).__setitem__(key, val)

    def __delitem__(self, key):
        """
        Override ``Counter.__delitem__()`` to invalidate the cached N
        """
        self._N = None
        super(FreqDist, self).__delitem__(key)

    def update(self, *args, **kwargs):
        """
        Override ``Counter.update()`` to invalidate the cached N
        """
        self._N = None
        super(FreqDist, self).update(*args, **kwargs)

    def setdefault(self, key, val):
        """
        Override ``Counter.setdefault()`` to invalidate the cached N
        """
        self._N = None
        super(FreqDist, self).setdefault(key, val)

    def B(self):
        """
        Return the total number of sample values (or "bins") that
        have counts greater than zero.  For the total
        number of sample outcomes recorded, use ``FreqDist.N()``.
        (FreqDist.B() is the same as len(FreqDist).)

        :rtype: int
        """
        return len(self)

    def hapaxes(self):
        """
        Return a list of all samples that occur once (hapax legomena)

        :rtype: list
        """
        return [item for item in self if self[item] == 1]

    def Nr(self, r, bins=None):
        return self.r_Nr(bins)[r]

    def r_Nr(self, bins=None):
        """
        Return the dictionary mapping r to Nr, the number of samples with frequency r, where Nr > 0.

        :type bins: int
        :param bins: The number of possible sample outcomes.  ``bins``
            is used to calculate Nr(0).  In particular, Nr(0) is
            ``bins-self.B()``.  If ``bins`` is not specified, it
            defaults to ``self.B()`` (so Nr(0) will be 0).
        :rtype: int
        """

        _r_Nr = defaultdict(int)
        for count in self.values():
            _r_Nr[count] += 1

        # Special case for Nr[0]:
        _r_Nr[0] = bins - self.B() if bins is not None else 0

        return _r_Nr

    def _cumulative_frequencies(self, samples):
        """
        Return the cumulative frequencies of the specified samples.
        If no samples are specified, all counts are returned, starting
        with the largest.

        :param samples: the samples whose frequencies should be returned.
        :type samples: any
        :rtype: list(float)
        """
        cf = 0.0
        for sample in samples:
            cf += self[sample]
            yield cf

    # slightly odd nomenclature freq() if FreqDist does counts and ProbDist does probs,
    # here, freq() does probs
    def freq(self, sample):
        """
        Return the frequency of a given sample.  The frequency of a
        sample is defined as the count of that sample divided by the
        total number of sample outcomes that have been recorded by
        this FreqDist.  The count of a sample is defined as the
        number of times that sample outcome was recorded by this
        FreqDist.  Frequencies are always real numbers in the range
        [0, 1].

        :param sample: the sample whose frequency
               should be returned.
        :type sample: any
        :rtype: float
        """
        n = self.N()
        if n == 0:
            return 0
        return self[sample] / n

    def max(self):
        """
        Return the sample with the greatest number of outcomes in this
        frequency distribution.  If two or more samples have the same
        number of outcomes, return one of them; which sample is
        returned is undefined.  If no outcomes have occurred in this
        frequency distribution, return None.

        :return: The sample with the maximum number of outcomes in this
                frequency distribution.
        :rtype: any or None
        """
        if len(self) == 0:
            raise ValueError(
                "A FreqDist must have at least one sample before max is defined."
            )
        return self.most_common(1)[0][0]

    def plot(self, *args, **kwargs):
        """
        Plot samples from the frequency distribution
        displaying the most frequent sample first.  If an integer
        parameter is supplied, stop after this many samples have been
        plotted.  For a cumulative plot, specify cumulative=True.
        (Requires Matplotlib to be installed.)

        :param title: The title for the graph
        :type title: str
        :param cumulative: A flag to specify whether the plot is cumulative (default = False)
        :type title: bool
        """
        try:
            import matplotlib.pyplot as plt
        except ImportError as e:
            raise ValueError(
                "The plot function requires matplotlib to be installed."
                "See http://matplotlib.org/"
            ) from e

        if len(args) == 0:
            args = [len(self)]
        samples = [item for item, _ in self.most_common(*args)]

        cumulative = _get_kwarg(kwargs, "cumulative", False)
        percents = _get_kwarg(kwargs, "percents", False)
        if cumulative:
            freqs = list(self._cumulative_frequencies(samples))
            ylabel = "Cumulative Counts"
            if percents:
                freqs = [f / freqs[len(freqs) - 1] * 100 for f in freqs]
                ylabel = "Cumulative Percents"
        else:
            freqs = [self[sample] for sample in samples]
            ylabel = "Counts"
        # percents = [f * 100 for f in freqs]  only in ProbDist?

        ax = plt.gca()
        ax.grid(True, color="silver")

        if "linewidth" not in kwargs:
            kwargs["linewidth"] = 2
        if "title" in kwargs:
            ax.set_title(kwargs["title"])
            del kwargs["title"]

        ax.plot(freqs, **kwargs)
        ax.set_xticks(range(len(samples)))
        ax.set_xticklabels([str(s) for s in samples], rotation=90)
        ax.set_xlabel("Samples")
        ax.set_ylabel(ylabel)

        plt.show()

        return ax

    def tabulate(self, *args, **kwargs):
        """
        Tabulate the given samples from the frequency distribution (cumulative),
        displaying the most frequent sample first.  If an integer
        parameter is supplied, stop after this many samples have been
        plotted.

        :param samples: The samples to plot (default is all samples)
        :type samples: list
        :param cumulative: A flag to specify whether the freqs are cumulative (default = False)
        :type title: bool
        """
        if len(args) == 0:
            args = [len(self)]
        samples = [item for item, _ in self.most_common(*args)]

        cumulative = _get_kwarg(kwargs, "cumulative", False)
        if cumulative:
            freqs = list(self._cumulative_frequencies(samples))
        else:
            freqs = [self[sample] for sample in samples]
        # percents = [f * 100 for f in freqs]  only in ProbDist?

        width = max(len("{}".format(s)) for s in samples)
        width = max(width, max(len("%d" % f) for f in freqs))

        for i in range(len(samples)):
            print("%*s" % (width, samples[i]), end=" ")
        print()
        for i in range(len(samples)):
            print("%*d" % (width, freqs[i]), end=" ")
        print()

    def copy(self):
        """
        Create a copy of this frequency distribution.

        :rtype: FreqDist
        """
        return self.__class__(self)

    # Mathematical operatiors

    def __add__(self, other):
        """
        Add counts from two counters.

        >>> FreqDist('abbb') + FreqDist('bcc')
        FreqDist({'b': 4, 'c': 2, 'a': 1})

        """
        return self.__class__(super(FreqDist, self).__add__(other))

    def __sub__(self, other):
        """
        Subtract count, but keep only results with positive counts.

        >>> FreqDist('abbbc') - FreqDist('bccd')
        FreqDist({'b': 2, 'a': 1})

        """
        return self.__class__(super(FreqDist, self).__sub__(other))

    def __or__(self, other):
        """
        Union is the maximum of value in either of the input counters.

        >>> FreqDist('abbb') | FreqDist('bcc')
        FreqDist({'b': 3, 'c': 2, 'a': 1})

        """
        return self.__class__(super(FreqDist, self).__or__(other))

    def __and__(self, other):
        """
        Intersection is the minimum of corresponding counts.

        >>> FreqDist('abbb') & FreqDist('bcc')
        FreqDist({'b': 1})

        """
        return self.__class__(super(FreqDist, self).__and__(other))

    def __le__(self, other):
        """
        Returns True if this frequency distribution is a subset of the other
        and for no key the value exceeds the value of the same key from
        the other frequency distribution.

        The <= operator forms partial order and satisfying the axioms
        reflexivity, antisymmetry and transitivity.

        >>> FreqDist('a') <= FreqDist('a')
        True
        >>> a = FreqDist('abc')
        >>> b = FreqDist('aabc')
        >>> (a <= b, b <= a)
        (True, False)
        >>> FreqDist('a') <= FreqDist('abcd')
        True
        >>> FreqDist('abc') <= FreqDist('xyz')
        False
        >>> FreqDist('xyz') <= FreqDist('abc')
        False
        >>> c = FreqDist('a')
        >>> d = FreqDist('aa')
        >>> e = FreqDist('aaa')
        >>> c <= d and d <= e and c <= e
        True
        """
        if not isinstance(other, FreqDist):
            raise_unorderable_types("<=", self, other)
        return set(self).issubset(other) and all(
            self[key] <= other[key] for key in self
        )

    def __ge__(self, other):
        if not isinstance(other, FreqDist):
            raise_unorderable_types(">=", self, other)
        return set(self).issuperset(other) and all(
            self[key] >= other[key] for key in other
        )

    __lt__ = lambda self, other: self <= other and not self == other
    __gt__ = lambda self, other: self >= other and not self == other

    def __repr__(self):
        """
        Return a string representation of this FreqDist.

        :rtype: string
        """
        return self.pformat()

    def pprint(self, maxlen=10, stream=None):
        """
        Print a string representation of this FreqDist to 'stream'

        :param maxlen: The maximum number of items to print
        :type maxlen: int
        :param stream: The stream to print to. stdout by default
        """
        print(self.pformat(maxlen=maxlen), file=stream)

    def pformat(self, maxlen=10):
        """
        Return a string representation of this FreqDist.

        :param maxlen: The maximum number of items to display
        :type maxlen: int
        :rtype: string
        """
        items = ["{0!r}: {1!r}".format(*item) for item in self.most_common(maxlen)]
        if len(self) > maxlen:
            items.append("...")
        return "FreqDist({{{0}}})".format(", ".join(items))

    def __str__(self):
        """
        Return a string representation of this FreqDist.

        :rtype: string
        """
        return "<FreqDist with %d samples and %d outcomes>" % (len(self), self.N())

    def __iter__(self):
        """
        Return an iterator which yields tokens ordered by frequency.

        :rtype: iterator
        """
        for token, _ in self.most_common(self.B()):
            yield token


            
from itertools import islice, chain, combinations, tee            
            
def pad_sequence(
    sequence,
    n,
    pad_left=False,
    pad_right=False,
    left_pad_symbol=None,
    right_pad_symbol=None,
):
    """
    Returns a padded sequence of items before ngram extraction.

        >>> list(pad_sequence([1,2,3,4,5], 2, pad_left=True, pad_right=True, left_pad_symbol='<s>', right_pad_symbol='</s>'))
        ['<s>', 1, 2, 3, 4, 5, '</s>']
        >>> list(pad_sequence([1,2,3,4,5], 2, pad_left=True, left_pad_symbol='<s>'))
        ['<s>', 1, 2, 3, 4, 5]
        >>> list(pad_sequence([1,2,3,4,5], 2, pad_right=True, right_pad_symbol='</s>'))
        [1, 2, 3, 4, 5, '</s>']

    :param sequence: the source data to be padded
    :type sequence: sequence or iter
    :param n: the degree of the ngrams
    :type n: int
    :param pad_left: whether the ngrams should be left-padded
    :type pad_left: bool
    :param pad_right: whether the ngrams should be right-padded
    :type pad_right: bool
    :param left_pad_symbol: the symbol to use for left padding (default is None)
    :type left_pad_symbol: any
    :param right_pad_symbol: the symbol to use for right padding (default is None)
    :type right_pad_symbol: any
    :rtype: sequence or iter
    """
    sequence = iter(sequence)
    if pad_left:
        sequence = chain((left_pad_symbol,) * (n - 1), sequence)
    if pad_right:
        sequence = chain(sequence, (right_pad_symbol,) * (n - 1))
    return sequence            
            
def ngrams(
    sequence,
    n,
    pad_left=False,
    pad_right=False,
    left_pad_symbol=None,
    right_pad_symbol=None,
):
    """
    Return the ngrams generated from a sequence of items, as an iterator.
    For example:

        >>> from nltk.util import ngrams
        >>> list(ngrams([1,2,3,4,5], 3))
        [(1, 2, 3), (2, 3, 4), (3, 4, 5)]

    Wrap with list for a list version of this function.  Set pad_left
    or pad_right to true in order to get additional ngrams:

        >>> list(ngrams([1,2,3,4,5], 2, pad_right=True))
        [(1, 2), (2, 3), (3, 4), (4, 5), (5, None)]
        >>> list(ngrams([1,2,3,4,5], 2, pad_right=True, right_pad_symbol='</s>'))
        [(1, 2), (2, 3), (3, 4), (4, 5), (5, '</s>')]
        >>> list(ngrams([1,2,3,4,5], 2, pad_left=True, left_pad_symbol='<s>'))
        [('<s>', 1), (1, 2), (2, 3), (3, 4), (4, 5)]
        >>> list(ngrams([1,2,3,4,5], 2, pad_left=True, pad_right=True, left_pad_symbol='<s>', right_pad_symbol='</s>'))
        [('<s>', 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, '</s>')]


    :param sequence: the source data to be converted into ngrams
    :type sequence: sequence or iter
    :param n: the degree of the ngrams
    :type n: int
    :param pad_left: whether the ngrams should be left-padded
    :type pad_left: bool
    :param pad_right: whether the ngrams should be right-padded
    :type pad_right: bool
    :param left_pad_symbol: the symbol to use for left padding (default is None)
    :type left_pad_symbol: any
    :param right_pad_symbol: the symbol to use for right padding (default is None)
    :type right_pad_symbol: any
    :rtype: sequence or iter
    """
    sequence = pad_sequence(
        sequence, n, pad_left, pad_right, left_pad_symbol, right_pad_symbol
    )

    history = []
    while n > 1:
        # PEP 479, prevent RuntimeError from being raised when StopIteration bubbles out of generator
        try:
            next_item = next(sequence)
        except StopIteration:
            # no more data, terminate the generator
            return
        history.append(next_item)
        n -= 1
    for item in sequence:
        history.append(item)
        yield tuple(history)
        del history[0]            


import pickle
# 将对象以二进制形式保存
def save(filename, cls):
        with open(filename, 'wb') as f:
            pickle.dump(cls, f)


# 加载二进制形式的对象
def load(filename):
    with open(filename, 'rb') as f:
        cls = pickle.load(f)
        return cls            
            
# get the sequence with 3:1
def standard_log_key(log_key_sequence_str):
    # 将日志键， 通过滑动窗口分为一个一个日志序列，这里将其分为4个日志键为一个序列
    tokens = log_key_sequence_str.split(' ')
    # 将日志键其变为int
    tokens = [int(i) for i in tokens]
    K = max(tokens)+1  # 日志键的种类个数
    # print("the tokens are:",tokens)
    bigramfdist_4 = FreqDist()
    bigrams_4 = ngrams(tokens, 4)
    # from nltk.util import ngrams
    # a = ['1', '2', '3', '4', '5']
    # b = ngrams(a, 2)
    # for i in b:
    #     print
    #     i
    # ('1', '2')
    # ('2', '3')
    # ('3', '4')
    # ('4', '5')
    bigramfdist_4.update(bigrams_4)
#     print("the bigramfdsit_4 is:", list(bigramfdist_4.keys()))
    # we set the length of history logs as 3
    seq = np.array(list(bigramfdist_4.keys()))

    # print("the seq is:",seq)
    X, Y = seq[:, :3], seq[:, 3:4]
    # print(seq.shape)   # (253, 4)
    # print(X_normal.shape)  # (253, 3)
    # print(Y_normal.shape)  # (253, 1)
    X = np.reshape(X, (-1, 3, 1))
    # print(X_normal)
    # [[[6]
    #   [72]
    #   [6]]
    #
    #  [[72]
    #     [6]
    #     [6]]
    #  ...]
    # 将数字等比缩小，变为从0到1
    X = X / K
    # 将整型标签转为onehot
    num_classes = len(list(set(Y.T.tolist()[0]))) + 1 # num_classes指的是Y_normal的种类
    Y = keras.utils.to_categorical(Y)   # num_classes=num_classes
    return X, Y


def execution_path(df_train_log, df_test_log):
    # 提取normal日志键执行流
    train_log_key_sequence_str = " ".join([str(EventId) for EventId in df_train_log["EventId"]])
    # 将日志流通过滑动窗口分为4个日志为一个日志序列
    X_train, Y_train = standard_log_key(train_log_key_sequence_str)

    # 对日志序列进行lstm训练和测试
    lg = log_key_model()
    execution_path_model_filepath = "ExecutePathModel.pkl"
    if os.path.isfile(execution_path_model_filepath):
        model = load(execution_path_model_filepath)
    else:
        model = lg.train(X_train, Y_train)
        save(execution_path_model_filepath, model)


    # 提取test日志键执行流
    test_log_key_sequence_str = " ".join([str(EventId) for EventId in df_test_log["EventId"]])
    # 将日志流通过滑动窗口分为4个日志为一个日志序列
    X_test, Y_test = standard_log_key(test_log_key_sequence_str)

    anomaly_sequence = lg.predcit(model, X_test, Y_test)
    print('the length of anomaly_sequence {} is {}'.format(anomaly_sequence, len(anomaly_sequence)))

    # anomaly_sequence为异常的序列
    anormal_lineid_list = [i+10 for i in anomaly_sequence]
    df_anormal = df_test_log.loc[df_test_log["LineId"].isin(anormal_lineid_list)]
    df_anormal.to_csv("execute_path_anormal.csv", index=False)
    
    