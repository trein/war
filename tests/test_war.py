import unittest
import mock
import war


class CounterProcessor(war.ChunkProcesssor):
    def process_chunk(self, core, chunk):
        for i in chunk:
            yield i + 1


class LineProcessor(war.ChunkProcesssor):
    def process_chunk(self, core, chunk):
        for line in chunk:
            yield 'cu' + line.strip() + 'cu\n'


class InMemoryTaskTest(unittest.TestCase):
    def test_chunk_iterator_with_empty_iterable(self):
        partitions = 4
        iterable = []
        self.assertRaises(Exception, war.InMemoryTask, (mock.Mock(), iterable, partitions))

    def test_chunk_iterator_with_iterable_smaller_than_partitions(self):
        partitions = 4
        iterable = range(5)
        task = war.InMemoryTask(mock.Mock(), iterable, partitions)
        chunk_id, first_chunk = task.chunk_iterator().next()
        self.assertEqual(chunk_id, 0)
        self.assertEqual(len(first_chunk), 1)

    def test_chunk_iterator_with_iterable_larger_than_partitions(self):
        partitions = 5
        iterable = range(10)
        task = war.InMemoryTask(mock.Mock(), iterable, partitions)
        chunk_id, first_chunk = task.chunk_iterator().next()
        self.assertEqual(chunk_id, 0)
        self.assertEqual(len(first_chunk), 2)

    def test_chunk_size(self):
        iterable = range(3)

        partitions = 5
        task = war.InMemoryTask(mock.Mock(), iterable, partitions)
        self.assertEqual(task.chunk_size(), 5)

        partitions = 1
        task = war.InMemoryTask(mock.Mock(), iterable, partitions)
        self.assertEqual(task.chunk_size(), 1)

        partitions = 0
        self.assertRaises(Exception, war.InMemoryTask, (mock.Mock(), iterable, partitions))

    def test_chunk_size_with_iterable_larger_than_partitions(self):
        partitions = 5
        iterable = range(10)
        task = war.InMemoryTask(mock.Mock(), iterable, partitions)
        self.assertEqual(task.chunk_size(), 5)

    def test_chunk_process(self):
        partitions = 5
        iterable = range(10)
        processor = mock.Mock()
        processor.process_chunk.return_value = range(partitions)
        task = war.InMemoryTask(processor, iterable, partitions)
        war.run(task)
        self.assertEqual(processor.process_chunk.call_count, 5)

    def test_outputs_are_processed(self):
        pass
