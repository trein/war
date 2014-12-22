import os
import logging
import multiprocessing

logger = logging.getLogger('war')


def run(task):
    dispatcher = TaskDispatcher()
    dispatcher.run(task)


class ChunkProcesssor(object):
    def process_chunk(self, core, chunk):
        raise NotImplementedError('method should be implemented by subclasses')


class Task(object):
    def outputs(self):
        raise NotImplementedError('method should be implemented by subclasses')

    def process(self, core, chunk, job_result):
        raise NotImplementedError('method should be implemented by subclasses')

    def process_output(self, results):
        raise NotImplementedError('method should be implemented by subclasses')

    def chunk_size(self):
        raise NotImplementedError('method should be implemented by subclasses')

    def chunk_iterator(self):
        raise NotImplementedError('method should be implemented by subclasses')

    def cleanup(self):
        raise NotImplementedError('method should be implemented by subclasses')


class InMemoryTask(Task):
    def __init__(self, processor, iterable, partitions):
        self._processor = processor
        self._chunks = self._grouper(iterable, partitions)
        self._partitions = partitions
        self._outputs = []

    def _grouper(self, iterable, partitions):
        if not partitions:
            raise Exception('partitions should be greater than 0')

        if not iterable:
            raise Exception('empty iterable')

        chunks = []
        chunk_size = float(len(iterable)) / float(partitions) + 1e-6

        if chunk_size == 0:
            raise Exception('not enough items to distribute')

        start_index = 0
        end_index = int(chunk_size)
        increment = chunk_size
        while end_index <= len(iterable):
            chunk = iterable[start_index:end_index]
            chunks.append(chunk)
            start_index = end_index
            increment += chunk_size
            end_index = int(increment)
            logger.debug('Created chunk [%s] with index %s:%s - %s', chunk, start_index, end_index, increment)

        return chunks

    def chunk_size(self):
        return self._partitions

    def chunk_iterator(self):
        for partition, chunk in zip(xrange(self._partitions), self._chunks):
            yield partition, chunk

    def process(self, core, chunk, job_result):
        results = []
        for entry in self._processor.process_chunk(core, chunk):
            results.append(entry)
        job_result[core] = results

    def process_output(self, results):
        for out in results.values():
            self._outputs.append(out)

    def cleanup(self):
        pass

    def outputs(self):
        return self._outputs


class FSTask(Task):
    def __init__(self, processor, filename, partitions):
        self._processor = processor
        self._input_filename = filename
        self._output_filename = filename + '.out'
        self._partitions = partitions
        self._in_chunk_filenames, self._out_chunk_filenames = self._create_chunks(filename, partitions)

    def _create_chunks(self, filename, partitions):
        base_chuck_filename = filename + '.part'
        in_chunk_filenames = [base_chuck_filename + str(index) for index in xrange(partitions)]
        out_chunk_filenames = [base_chuck_filename + '.temp' + str(index) for index in xrange(partitions)]

        with open(filename, 'r') as source:
            for line in source:
                index = hash(line) % partitions
                chunk_filename = in_chunk_filenames[index]
                with open(chunk_filename, 'a') as destination:
                    destination.write(line)

        return in_chunk_filenames, out_chunk_filenames

    def chunk_size(self):
        return self._partitions

    def chunk_iterator(self):
        for partition, chunk_filename in zip(xrange(self._partitions), self._in_chunk_filenames):
            yield partition, chunk_filename

    def process(self, core, input_chunk_filename, job_result):
        output_chunk_filename = self._out_chunk_filenames[core]
        with open(input_chunk_filename, 'r') as in_chunk, open(output_chunk_filename, 'w') as out_chunk:
            for entry in self._processor.process_chunk(core, in_chunk):
                out_chunk.write(entry)
        job_result[core] = output_chunk_filename

    def process_output(self, results):
        with open(self._output_filename, 'w') as destination:
            for output_chunk_filename in results.values():
                with open(output_chunk_filename, 'r') as source:
                    for line in source:
                        destination.write(line)

    def cleanup(self):
        for output_chunk_filename in self._out_chunk_filenames:
            os.remove(output_chunk_filename)

        for input_chunk_filename in self._in_chunk_filenames:
            os.remove(input_chunk_filename)

    def outputs(self):
        return self._output_filename


class TaskDispatcher(object):
    def run(self, task):
        try:
            pid = os.getpid()
            logger.info('Main PID: %s', pid)

            results = multiprocessing.Manager().dict()
            jobs = []

            logger.info('Processing chunks')
            chunks = task.chunk_iterator()
            for partition, chunk in chunks:
                job = multiprocessing.Process(target=task.process, args=(partition, chunk, results))
                jobs.append(job)
                job.start()

            logger.info('Waiting processes')
            for job in jobs:
                job.join()

            logger.info('Processing output')
            task.process_output(results)
        finally:
            logger.info('Cleaning up')
            task.cleanup()
