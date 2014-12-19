import sys
import logging
sys.path.insert(0, '../war')
import war

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


class CounterProcessor(war.ChunkProcesssor):
    def process_chunk(self, core, chunk):
        for i in chunk:
            yield i + 1


class LineProcessor(war.ChunkProcesssor):
    def process_chunk(self, core, chunk):
        for line in chunk:
            yield 'cu' + line.strip() + 'cu'


task = war.InMemoryTask(CounterProcessor(), range(100), 4)
war.run(task)
print 'Results:', task.outputs()

task = war.FSTask(LineProcessor(), 'tests/test.csv', 4)
war.run(task)
print 'Results:', task.outputs()