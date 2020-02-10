import unittest

from bobtools.parallel import funnel


class funnelTest(unittest.TestCase):
    def test_lambdas_to_generator(self):

        data = range(10)
        paralel_func = lambda x: x * 2  # noqa
        single_func = lambda x: x / 2  # noqa

        res = list(funnel(data, paralel_func, single_func))

        self.assertEqual(len(res), len(range(10)))
        self.assertEqual(sum(res), sum(range(10)))

    def test_single_without_output(self):
        def gen():
            for i in range(10):
                yield i

        g = gen()

        parallel_func = lambda x: x  # noqa
        single_func = lambda x: None  # noqa

        res = list(
            funnel(iterable=g, multi_func=parallel_func, single_func=single_func)
        )
        self.assertRaises(StopIteration, g.__next__)
        self.assertEqual(len(res), 0)

    def test_no_single_func(self):
        data = range(10)
        parallel_func = lambda x: x * 2  # noqa

        res = list(funnel(data, parallel_func, timeout=10))

        self.assertEqual(len(res), 10)

    def test_positional_arguments(self):
        def with_positional():
            for i in range(10):
                yield (i, 2)

        def parallel_func(x, y):
            """pass a positional argument (3) to the single func"""
            return x * y, 3

        single_func = lambda x, y: x / y  # noqa

        res = list(funnel(with_positional(), parallel_func, single_func))
        expected = [i * 2 / 3 for i in range(10)]
        self.assertAlmostEqual(sum(res), sum(expected))
        self.assertEqual(len(res), len(expected))

    def test_named_arguments(self):
        def with_named():
            for i in range(10):
                yield {"x": i, "y": 2, "z": 4}

        parallel_func = lambda x, y=1, z=3: {"x": x * z, "z": 3}  # noqa
        single_func = lambda z=1, y=2, x=3: x / z  # noqa

        res = list(funnel(with_named(), parallel_func, single_func))
        expected = [i * 4 / 3 for i in range(10)]

        self.assertEqual(sum(res), sum(expected))
        self.assertEqual(len(res), len(expected))

    def test_assert_minimum_check(self):

        data = range(10)
        multi_func = lambda x: x  # noqa
        single_func = lambda x: x  # noqa

        self.assertRaises(
            ValueError, funnel(data, multi_func, single_func, n_workers=1).__next__
        )

    def test_dict_values(self):
        from bobtools.parallel import funnel

        data = [{"id": 1}, {"id": 2}]

        def datagen():
            for i in data:
                yield (i,)

        def multi_func(thing):
            thing["id"] += 1
            print(thing)
            return (thing,)

        class Toucher:
            n_touched = 0
            id_sum = 0

            def touch(self, thing):
                self.n_touched += 1
                self.id_sum += thing["id"]
                return self.id_sum

        toucher = Toucher()

        res = list(funnel(datagen(), multi_func, toucher.touch))
        self.assertEqual(res[-1], sum([thing["id"] + 1 for thing in data]))


if __name__ == "__main__":
    unittest.main()
